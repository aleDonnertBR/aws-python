import boto3, json, datetime, os
from botocore.exceptions import ClientError
import csv
import os
import sys
from os import walk
import time

#Parâmetros para execuções 
s3client = boto3.client("s3")
s3_resource = boto3.resource('s3')
local_dir = '/tmp/'
s3 = boto3.resource('s3')
original = 'detran/cpfs.txt'

emr = boto3.client('emr', region_name='us-east-1')

#Download do arquivo cpfs.txt do bucket de origem
try:
    s3client.download_file('modelagem',original,f'{local_dir}cpf.txt')
except ClientError as e:
    if e.response['Error']['Code'] == "404":
        print('Arquivo cpfs.txt inexistente no bucket: s3://modelagem/detran/')
        sys.exit(0)
    else:
        sys.exit(0)
#Parâmetros para split do arquivo cpfs.txt em partições
SOURCE_FILEPATH = f'{local_dir}cpf.txt'
DEST_PATH = f'{local_dir}'
RESULT_FILENAME_PREFIX = 'cpfs_split'

#Função para upload dos arquivos 'splitados' em bucket de leitura
def upload_to_s3(arquivo,bucket,key):

    try:
        s3.meta.client.upload_file(Filename=arquivo, Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False  
#Função para split do arquivo de origem em arquivos por partição
def split_csv(source_filepath, dest_path, result_filename_prefix, row_limit):
  
    bucket='modelagem'
    sufixo = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    key_original = f'{local_dir}cpf_{sufixo}.txt' 
    #Valido a quantidade de linhas para split, com a necessidade de pelo menos uma  
    if row_limit <= 0:
        raise Exception('row_limit must be > 0')
    #Leitura do arquivo baixado do bucket e salvo no /tmp do lambda
    with open(source_filepath, 'r') as source:
        reader = csv.reader(source)
        headers = next(reader)

        file_number = 0
        records_exist = True
        #incremento do arquivo conforme existam linhas 
        while records_exist:

            i = 0
            target_filename = f'{result_filename_prefix}_{file_number}.csv'
            target_filepath = os.path.join(dest_path, target_filename)
            arquivo = f'{local_dir}{result_filename_prefix}_{file_number}.csv'
            key = f'detran/{target_filename}'
            key_original = f'detran/cpfs_{sufixo}.txt'
            
            with open(target_filepath, 'w') as target:
                writer = csv.writer(target)

                while i < row_limit:
                    if i == 0:
                        writer.writerow(headers)

                    try:
                        writer.writerow(next(reader))
                        i += 1
                    except:
                        records_exist = False
                        break
            #upload do arquivo split_versao para o bucket de leitura do processo de selenium/captcha
            upload_to_s3(arquivo,bucket,key)
            file_number += 1
        #Upload do arquivo cpfs.txt com seu respectivo timestamp de execução para histórico no bucket de origem    
        upload_to_s3(SOURCE_FILEPATH,bucket,key_original)
        #Delete do arquivo cpfs.txt após armazenamento do seu histórico
        s3.Object(bucket, original).delete()
        #Retorno a quantidade de partições que será passada para o processo de criação dos steps de execução em paralelo
        return file_number

def lambda_handler(event, context):
    
    file = open(SOURCE_FILEPATH)
    reader = csv.reader(file)
    row_count= len(list(reader))
    #Valido que o arquivo de origem não tenha mais de 10 mil linhas devido ao tempo de execução
    if row_count > 10000:
        print('Execução não iniciada devido ao volume acima de 10000 registros.')
        sys.exit(0)
    else:
        if row_count < 25:
            ROW_LIMIT = 5
        else:
            ROW_LIMIT = int(row_count/5)

    print('Total de linhas do arquivo: ', row_count)
    print('Linhas por partition: ', ROW_LIMIT)
    
    partition =split_csv(SOURCE_FILEPATH, DEST_PATH, RESULT_FILENAME_PREFIX, ROW_LIMIT)
    
    steps = []
    dict_split_file = {}
    #Adiciono um step para cada arquivo_split o qual retornou via file_number com o numero de partições
    for i in range(partition):
        steps.append(
    {
        'Name': f'detran-{i}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--py-files', 
                's3://modelagem/omni-datalake/detran/detran.zip', 
                's3://modelagem/omni-datalake/detran/scrapping_batch_alone.py',
                '-i', str(i)
                ]
            }
        }
        )
    #Adiciono por último o step de consolidação dos arquivos split para saída única no bucket de retorno
    steps.append(
    {
        'Name': 'consolida',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--py-files', 
                's3://modelagem/omni-datalake/detran/detran.zip', 
                's3://modelagem/omni-datalake/detran/consolida_batch_alone.py',
                '-i', str(partition)
                ]
            }
    }
    )
    #Criação do cluste EMR para execuções em paralelo do processo de captcha do Detran
    response = emr.run_job_flow(
    Name='detran',
    LogUri='s3://modelagem/omni-datalake/infrastructure/logs/emr/testeDetran',
    ReleaseLabel='emr-6.3.0',
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 2,
        'EmrManagedSlaveSecurityGroup': 'sg-0d91939504dc11210',
        'EmrManagedMasterSecurityGroup': 'sg-0452edec6c990f2fb',
        'Ec2SubnetId': 'subnet-01c22397eb70ecd57',
        "ServiceAccessSecurityGroup": "sg-0225c2a4d6fa4fa87",
        'TerminationProtected': False
    },
    Applications=[
        { "Name": "Hadoop" }, 
        { "Name": "Spark" }, 
        { "Name": "Hive" }, 
        { "Name": "Ganglia" }
    
    ],
    BootstrapActions=[
        {
            'Name': 'Install Python packages',
            'ScriptBootstrapAction': {
                'Path': 's3://modelagem/omni-datalake/jobs/python_libraries_detran.sh'
            }
        }
    ],
    StepConcurrencyLevel=5,
    Steps=steps,
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole',
    Configurations=[
        {
            'Classification': 'spark',
            'Properties': {
                'maximizeResourceAllocation': 'true'
                }
            },
        ],
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Sucesso!')
    }
