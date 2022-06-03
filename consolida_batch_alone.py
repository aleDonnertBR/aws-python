# -*- coding: utf-8 -*-
"""
Created on Wed Nov 24 15:28:19 2021

@author: alexandre.somogyi
"""

import codecs
import boto3
from os.path import isfile
from botocore.exceptions import ClientError
import time
import sys 


arquivo = 'detran_cpfs_validados.csv'
bucket = 'modelagem'
key = 'detran/processed-data/detran_cpfs_validados.csv'

s3 = boto3.resource('s3')
emr = boto3.client('emr', region_name='us-east-1')

retorno = sys.argv[2]
particoes = int(retorno)

def aguarda_execucao(arquivo,bucket,key):
    cluster_name = 'detran'
    clusters = emr.list_clusters()
    your_cluster = [i for i in clusters['Clusters'] if i['Name'] == cluster_name][0]
    cluster = emr.describe_cluster(ClusterId=your_cluster['Id'])
    status = cluster['Cluster']['Status']['State']

    status_response = emr.list_steps(ClusterId=your_cluster['Id'])
    steps = status_response['Steps']
    
    contador = 0
    print('Particoes: ', particoes)
    
    for i in steps:
        nome = i['Name']
        status = i['Status']['State']
        print(nome)
        print(status)
        if 'detran' in nome.strip():
            if status != 'COMPLETED':
                time.sleep(100)
                print('Ainda nao')
                aguarda_execucao(arquivo,bucket,key)
            else:
                contador += 1
                print('Contador: ', contador)
                if contador == particoes:
                    print('AGORA FOI!!!!: ',contador)
                    itera_arquivos(arquivo,bucket,key)


def itera_arquivos(arquivo,bucket,key):
    lista = []
    line_stream = codecs.getreader("utf-8")
    bucket_iter = s3.Bucket('modelagem')
    prefix_objs = bucket_iter.objects.filter(Prefix="detran/processed-data/cpfs_validados_detran_")
    prefix_split_objs = bucket_iter.objects.filter(Prefix="detran/cpfs_split_")
    
    for obj in prefix_objs:
        for line in line_stream(obj.get()['Body']):
            #print(line)
            lista.append(line)
    
    conjunto = set(lista)
    lista_final = list(conjunto)
    
    joined_string = "".join(lista_final)
    text_out = str(joined_string)
    print(text_out)
    save_file(arquivo,text_out,bucket,key,prefix_objs,prefix_split_objs)

def save_file(arquivo,text_out,bucket,key,prefix_objs,prefix_split_objs):
    print(text_out)
    if not isfile(arquivo):
        # Abre o arquivo para adicionar o texto
        file_object = open(arquivo, 'a')
        file_object.close()
    # Abre o arquivo para adicionar o texto
    file_object = open(arquivo, 'a')
    # Insere os valores
    file_object.write(text_out)
    # Fecha o arquivo
    file_object.close()
    upload_to_s3(arquivo,bucket,key,prefix_objs,prefix_split_objs)
    

# Upload file to S3  
def upload_to_s3(arquivo,bucket,key,prefix_objs,prefix_split_objs):
    s3 = boto3.resource('s3')
    try:
        print(arquivo)
        print(bucket)
        print(key)
        s3.meta.client.upload_file(Filename=arquivo, Bucket=bucket, Key=key)
        deleta_partitions(bucket,prefix_objs,prefix_split_objs)
        return True
    except ClientError:
        return False   
    
def deleta_partitions(bucket,prefix_objs,prefix_split_objs):
    for obj in prefix_objs:
        chave = obj.key
        print(chave)
        s3.Object(bucket, chave).delete()
        
    for obj in prefix_split_objs:
        chave_split = obj.key
        print(chave_split)
        s3.Object(bucket, chave_split).delete() 
    print('Fechando!')    
    sys.exit(0)
    
aguarda_execucao(arquivo,bucket,key)