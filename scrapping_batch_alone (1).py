from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException  
from selenium.common.exceptions import TimeoutException as te
from selenium.common.exceptions import WebDriverException as wde
from botocore.exceptions import ClientError
import base64
from detran.resolver_captcha_alone import quebrar_captcha
import cv2
import os
import boto3
import csv
from os.path import isfile
import sys 
sys.path.append('/tmp')

bucket = 'modelagem'
local_dir = '/tmp/'
download_dir = '/mnt1/'
s3_path = 'detran/'


chrome_driver = 'chromedriver'
headless_chromium = 'headless-chromium'
versao = sys.argv[2]
url = 'http://www.detran.pa.gov.br/servicos/p_habilitacao/index.php'
arquivo = f'{local_dir}cpfs_validados_detran_{versao}.csv'
key = f'detran/processed-data/cpfs_validados_detran_{versao}.csv' 
file = 'cpf_{versao}.txt'
nome_arquivo_rotulo = f'{download_dir}rotulos_modelo.dat'
nome_arquivo_modelo = f'{download_dir}modelo_treinado.hdf5'
s3client = boto3.client("s3")
s3_resource = boto3.resource('s3')

try:
    s3_resource.Object('modelagem', f'detran/cpfs_split_{versao}.csv').load()
    s3client.download_file('modelagem',f'detran/cpfs_split_{versao}.csv','cpf_{versao}.txt')
except ClientError as e:
    if e.response['Error']['Code'] == "404":
        print('Arquivo inexistente')
        sys.exit(0)
    else:
        sys.exit(0)

def lambda_handler(bucket, chrome_driver, headless_chromium, url, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir, file):
    return input_lambda(bucket, chrome_driver, headless_chromium, url, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir, file)
       
def save_file(arquivo,dados,bucket,key):

    if not isfile(arquivo):
        # Abre o arquivo para adicionar o texto
        file_object = open(arquivo, 'a')
        file_object.close()
    # Abre o arquivo para adicionar o texto
    file_object = open(arquivo, 'a')
    # Insere os valores
    text_out = '\n'.join(';'.join(elems) for elems in dados)
    text_out = text_out + "\n"
    #text_out = ";".join(dados) + "\n"
    #print(text_out)
    file_object.write(text_out)  
    # Fecha o arquivo
    file_object.close()
    upload_to_s3(arquivo,bucket,key)

# Upload file to S3  
def upload_to_s3(arquivo,bucket,key):
    s3 = boto3.resource('s3')
    try:
        print(arquivo)
        print(bucket)
        print(key)
        s3.meta.client.upload_file(Filename=arquivo, Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False   

def delete_s3_file(bucket,nome_arquivo_temp):
    #s3 = boto3.client('s3')#boto3.resource('s3')
    try:
        #s3.delete_object(Bucket=bucket, Key=nome_arquivo_temp)
        os.remove(nome_arquivo_temp)
        print('Removeu: ',nome_arquivo_temp)
        return True
    except:
        return False   
    
def input_lambda(bucket, chrome_driver, headless_chromium, url, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir, file):   
    with open(file) as csvfile:
        cpfs = csv.reader(csvfile, delimiter='\n')
        for cpf in cpfs:
            cpf = ''.join(cpf)
            iCount = 0
            iterator = 0
            while iterator < 1:
                iCount += 1
                mensagem = baixa_captcha(url, cpf, bucket, chrome_driver, headless_chromium, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir)
                print('tentativas', iCount, 'msg: ', mensagem)
                if mensagem != "2" and mensagem != "3":
                    lista_saida = []
                    lista_resultado = []
                    lista_final = []
                    lista_saida.append(cpf)
                    lista_resultado.append(mensagem)
                    #set_tentativa()
                    iterator = 1
                    a_zip = zip(lista_saida, lista_resultado)
                    lista_final = list(a_zip)
                    print('Lista Final: ',lista_final)
                    save_file(arquivo,lista_final,bucket,key)
                elif mensagem == "3" and iCount >= 200:
                    print('Site fora do ar. Tente novamente em algumas horas.')
                    sys.exit(0)


def print_tentativas_certas_habilitado():
    return "1"#CPF POSSUI CNH
    
def print_tentativas_certas():
    return "0"#CPF NÃO POSSUI CNH
    
def print_tentativas_erradas_captcha():
    return "2"

def print_tentativas_erradas_cpf():
    return "-1"#CPF INVALIDO

def baixa_captcha(url, cpf, bucket, chrome_driver, headless_chromium, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir):
    nome_arquivo_temp = f'{local_dir}{cpf}.jpg'
    
    capabilities = webdriver.DesiredCapabilities.CHROME
    capabilities['acceptSslCerts'] = True
    
    options = Options()
    options.add_argument("window-size=1400,1500")
		
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument('--headless')
    options.add_argument('--single-process')
    options.add_argument('--disable-dev-shm-usage')
    options.accept_insecure_certs = True
    options.add_experimental_option('w3c', True)
    options.binary_location = download_dir + headless_chromium
    driver = webdriver.Chrome(
        executable_path = download_dir + chrome_driver,
        desired_capabilities=capabilities,
        chrome_options=options
    )
    
    try:
        driver.get(url)
    except te:
        mensagem = '3'
        return mensagem
			
    click_emissao = '//*[@id="profile"]/form/table/tbody/tr/td/table/tbody/tr[1]/td/div[1]/div[1]/label/span'
    digita_cpf = '//*[@id="fCPF"]'
    digita_captcha = '//*[@id="profile"]/form/table/tbody/tr/td/table/tbody/tr[1]/td/div[5]/div/input[1]'
    submit = '//*[@id="profile"]/form/table/tbody/tr/td/div/div/input[1]'
    captcha = '//*[@id="profile"]/form/table/tbody/tr/td/table/tbody/tr[2]/td/img'
    
    try:
        img = driver.find_element_by_xpath(captcha)
        
        img_base64 = driver.execute_script("""
		var ele = arguments[0];
		var cnv = document.createElement('canvas');
		cnv.width = ele.width; cnv.height = ele.height;
		cnv.getContext('2d').drawImage(ele, 0, 0);
		return cnv.toDataURL('image/jpeg').substring(22);    
		""", img)
        
        with open(nome_arquivo_temp, 'wb') as f:
            f.write(base64.b64decode(img_base64))
        
        f.close()
    except NoSuchElementException:
        mensagem = '3'
        return mensagem 

    captcha_img = cv2.imread(f'{nome_arquivo_temp}')
    captcha_img = captcha_img[10:100, 0:200]
    delete_s3_file(bucket,nome_arquivo_temp)
	
    resposta, img_captcha_tratado, img_captcha_letras = quebrar_captcha(imagem_captcha=captcha_img, nome_arquivo_rotulo=nome_arquivo_rotulo, nome_arquivo_modelo=nome_arquivo_modelo)
    
    print(resposta)
    
    try:
        driver.find_element_by_xpath(click_emissao).click()
        driver.find_element_by_xpath(digita_cpf).send_keys(cpf)
        driver.find_element_by_xpath(digita_captcha).send_keys(resposta)
        driver.find_element_by_xpath(submit).click()
    
        current_url = driver.current_url
        
    except  (NoSuchElementException, te, wde):
        mensagem = '3'
        return mensagem 
			
    if 'mostra_dados' in current_url: #Sem CPF Cadastrado
        mensagem = print_tentativas_certas()
        # return mensagem
        #baixa_captcha(webdriver_folder, url)
		
    elif 'senha' in current_url: #Erro de captcha
        mensagem = print_tentativas_erradas_captcha()

    elif 'POSSUI' in current_url:  #CPF possui habilitação
        mensagem = print_tentativas_certas_habilitado()
        
    elif 'DIGITO' in current_url: #CPF errado
        mensagem = print_tentativas_erradas_cpf()
    
    else:
        mensagem = '3'
        
    driver.close()
    return mensagem    

lambda_handler(bucket, chrome_driver, headless_chromium, url, arquivo, key, nome_arquivo_rotulo, nome_arquivo_modelo, local_dir, file)
