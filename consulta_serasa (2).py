
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 10:08:05 2022

@author: alexandre.somogyi

Script contendo as classes e funções utilizadas para a execução de pesquisa do Serasa pelo Motor.
"""

# Bibliotecas necessárias para a execução

#motor_uuid

#os.environ['motor_uuid'] = str(uuid.uuid4()) - main/loop



import sys
sys.path.append('./projects/omni-motor-cloud-lambda/assets/motor.zip')

import datetime
import boto3
import xml.etree.cElementTree as et
from botocore.exceptions import ClientError
from motor.motor_biblioteca import Motor
import requests
import time
import os
import re
import motor.dicionario_serasa
import pytz
import uuid
import io
import csv
from pprint import pprint
import pprint
import json


# bucket = os.getenv('bucket_infra')
# api_link = 'https://omni.confirmeonline.com.br/Integracao/Consulta?wsdl' # Atual

# ini = datetime.datetime.now()

# motor_obj = Motor()


class ConsultaSerasa():    
    
    def __init__(self, input_dict={}):
        self.dicionarioSerasa = motor.dicionario_serasa.dicionariosSerasa()        
        self.titularAtual = ""	
        self.input_file_values = input_dict
        self.inicializado = False
        self.blocos_nao_listados = []
        self.string_consulta = ''
        self.feature = []
        self.file_name = os.getenv('motor_uuid')
        self.bucket = 'modelagem'
        self.buff = io.BytesIO()
        self.buff_processed = io.BytesIO()
        self.teste = str(uuid.uuid4())
    
    #Abaixo os métodos que setam os dados de acordo com sua posição para os respectivos dicionários pertencentes aos blocos
    def popularB49C(self,bloco):
    
        self.dicionarioSerasa.registroB49C['numDocumentoConsultado'] = self.extrairCampo( 10, bloco, 15)
        self.dicionarioSerasa.registroB49C['tipoPessoaConsultado'] = self.extrairCampo( 25, bloco, 1)
        self.dicionarioSerasa.registroB49C['baseCons'] = self.extrairCampo( 26, bloco, 6)
        self.dicionarioSerasa.registroB49C['modalidade'] = self.extrairCampo( 32, bloco, 2)
        self.dicionarioSerasa.registroB49C['vlrConsul'] = self.extrairCampo( 34, bloco, 7)
        self.dicionarioSerasa.registroB49C['centroCusto'] = self.extrairCampo( 41, bloco, 12)
        self.dicionarioSerasa.registroB49C['codificado'] = self.extrairCampo( 53, bloco, 1)
        self.dicionarioSerasa.registroB49C['qtdRegistros'] = self.extrairCampo( 54, bloco, 2)
        self.dicionarioSerasa.registroB49C['conversa'] = self.extrairCampo( 56, bloco, 1)
        self.dicionarioSerasa.registroB49C['funcao'] = self.extrairCampo( 57, bloco, 3)
        self.dicionarioSerasa.registroB49C['tipoConsulta'] = self.extrairCampo( 60, bloco, 1)
        self.dicionarioSerasa.registroB49C['atualiza'] = self.extrairCampo( 61, bloco, 1)
        self.dicionarioSerasa.registroB49C['qtdCheque'] = self.extrairCampo( 104, bloco, 2)
        self.dicionarioSerasa.registroB49C['endTel'] = self.extrairCampo( 106, bloco, 1)
        self.dicionarioSerasa.registroB49C['querTel9Digitos'] = self.extrairCampo( 114, bloco, 1)
        self.dicionarioSerasa.registroB49C['ctaCorrente'] = self.extrairCampo( 115, bloco, 10)
        self.dicionarioSerasa.registroB49C['dgCtaCorrente'] = self.extrairCampo( 125, bloco, 1)
        self.dicionarioSerasa.registroB49C['agencia'] = self.extrairCampo( 126, bloco, 4)
        self.dicionarioSerasa.registroB49C['alerta'] = self.extrairCampo( 130, bloco, 1)
        self.dicionarioSerasa.registroB49C['logon'] = self.extrairCampo( 131,bloco, 8)
        self.dicionarioSerasa.registroB49C['viaInternet'] = self.extrairCampo( 139, bloco, 1)
        self.dicionarioSerasa.registroB49C['periodoCompro'] = self.extrairCampo( 141, bloco, 1)
        self.dicionarioSerasa.registroB49C['periodoEndereco'] = self.extrairCampo( 142, bloco, 1)
        self.dicionarioSerasa.registroB49C['qtdCompr'] = self.extrairCampo( 191, bloco, 2)
        self.dicionarioSerasa.registroB49C['negativos'] = self.extrairCampo( 193, bloco, 1)
        self.dicionarioSerasa.registroB49C['cheque'] = self.extrairCampo( 194, bloco, 1)
        self.dicionarioSerasa.registroB49C['dataConsul'] = self.extrairCampo( 195, bloco, 8)
        self.dicionarioSerasa.registroB49C['horaConsul'] = self.extrairCampo( 203, bloco, 6)
        self.dicionarioSerasa.registroB49C['totalReg'] = self.extrairCampo( 209, bloco, 4)
        self.dicionarioSerasa.registroB49C['qtdReg1'] = self.extrairCampo( 213, bloco, 4)
        self.dicionarioSerasa.registroB49C['codTab'] = self.extrairCampo( 217, bloco, 4)
        self.dicionarioSerasa.registroB49C['acessRechq'] = self.extrairCampo( 397, bloco, 1)
        self.dicionarioSerasa.registroB49C['temOcorrenciaRecheque'] = self.extrairCampo( 398, bloco, 1)
        self.dicionarioSerasa.registroB49C['reservado'] = self.extrairCampo( 399, bloco, 1)  
    
    def popularB001(self, bloco):
        self.titularAtual = self.extrairCampo(87,  bloco, 1)
        
        if (self.titularAtual == "S"):
            self.dicionarioSerasa.registroB001['tipo_reg'] = self.extrairCampo(0, bloco, 4)
            self.dicionarioSerasa.registroB001['grafia']         = self.extrairCampo(4,   bloco, 45)
            self.dicionarioSerasa.registroB001['cpf']            = self.extrairCampo(49,  bloco, 11)
            self.dicionarioSerasa.registroB001['rg']             = self.extrairCampo(60,  bloco, 15)
            self.dicionarioSerasa.registroB001['data_nasc']      = self.mascaraData(self.extrairCampo(75,  bloco, 8))      
            self.dicionarioSerasa.registroB001['cod_cidade']     = self.extrairCampo(83,  bloco, 4)
            self.dicionarioSerasa.registroB001['titular']        = self.titularAtual
            self.dicionarioSerasa.registroB001['ccf_ind']        = self.extrairCampo(88,  bloco, 1)
            self.dicionarioSerasa.registroB001['link_n']         = self.extrairCampo(92,  bloco, 9)
            self.dicionarioSerasa.registroB001['situaca_antiga'] = self.extrairCampo(101, bloco, 1)
            self.dicionarioSerasa.registroB001['dt_atual']       = self.mascaraData(self.extrairCampo(102, bloco, 8))
            self.dicionarioSerasa.registroB001['reservado']      = self.extrairCampo(110, bloco, 1)
            self.dicionarioSerasa.registroB001['indic_erro']     = self.extrairCampo(111, bloco, 1)
            self.dicionarioSerasa.registroB001['excluir_graf']   = self.extrairCampo(112, bloco, 1)
            self.dicionarioSerasa.registroB001['nova_situacao']  = self.extrairCampo(113, bloco, 1)        
        else:
            self.dicionarioSerasa.registroB001_outros['tipo_reg'].append(self.extrairCampo(0,   bloco, 4))
            self.dicionarioSerasa.registroB001_outros['grafia'].append(self.extrairCampo(4,   bloco, 45))
            self.dicionarioSerasa.registroB001_outros['cpf'].append(self.extrairCampo(49,  bloco, 11))
            self.dicionarioSerasa.registroB001_outros['rg'].append(self.extrairCampo(60,  bloco, 15))
            self.dicionarioSerasa.registroB001_outros['data_nasc'].append(self.mascaraData(self.extrairCampo(75,  bloco, 8)))
            self.dicionarioSerasa.registroB001_outros['cod_cidade'].append(self.extrairCampo(83,  bloco, 4))
            self.dicionarioSerasa.registroB001_outros['titular'].append(self.titularAtual)
            self.dicionarioSerasa.registroB001_outros['ccf_ind'].append(self.extrairCampo(88,  bloco, 1))
            self.dicionarioSerasa.registroB001_outros['link_n'].append(self.extrairCampo(92,  bloco, 9))
            self.dicionarioSerasa.registroB001_outros['situaca_antiga'].append(self.extrairCampo(101, bloco, 1))
            self.dicionarioSerasa.registroB001_outros['dt_atual'].append(self.mascaraData(self.extrairCampo(102, bloco, 8)))
            self.dicionarioSerasa.registroB001_outros['reservado'].append(self.extrairCampo(110, bloco, 1))
            self.dicionarioSerasa.registroB001_outros['indic_erro'].append(self.extrairCampo(111, bloco, 1))
            self.dicionarioSerasa.registroB001_outros['excluir_graf'].append(self.extrairCampo(112, bloco, 1))
            self.dicionarioSerasa.registroB001_outros['nova_situacao'].append(self.extrairCampo(113, bloco, 1))  
    
    			
    def popularA900(self,bloco, contador):

    	self.dicionarioSerasa.registroA900['tipo_reg'].append(self.extrairCampo( 0, bloco, 4))
    	self.dicionarioSerasa.registroA900['codigo'].append(self.extrairCampo( 4, bloco, 6))
    	self.dicionarioSerasa.registroA900['mens_reduz'].append(self.extrairCampo( 10, bloco, 32))
    	self.dicionarioSerasa.registroA900['mens_compl'].append(self.extrairCampo( 42, bloco, 70))
    	self.dicionarioSerasa.registroA900['filler'].append(self.extrairCampo( 112, bloco, 3))
    	self.dicionarioSerasa.registroA900['qtd_registros'] = contador
		
    def popularB002(self,bloco, contador):
    
    	self.dicionarioSerasa.registroB002['tipo_reg'].append(self.extrairCampo( 0, bloco, 4))
    	self.dicionarioSerasa.registroB002['filler_1'].append(self.extrairCampo( 4, bloco, 3))
    	self.dicionarioSerasa.registroB002['atualiza'].append(self.extrairCampo( 7, bloco, 8))
    	self.dicionarioSerasa.registroB002['data_nasc'].append(self.mascaraData(self.extrairCampo( 15, bloco, 8)))
    	self.dicionarioSerasa.registroB002['nome_mae'].append(self.extrairCampo( 23, bloco, 45))
    	self.dicionarioSerasa.registroB002['sexo'].append(self.extrairCampo(68, bloco, 1))
    	self.dicionarioSerasa.registroB002['tipo_doc'].append(self.extrairCampo( 69, bloco, 15))
    	self.dicionarioSerasa.registroB002['numdoc'].append(self.extrairCampo( 84, bloco, 15))
    	self.dicionarioSerasa.registroB002['orgao_emis'].append(self.extrairCampo(99, bloco, 5))
    	self.dicionarioSerasa.registroB002['data_emis'].append(self.mascaraData(self.extrairCampo(104, bloco, 8)))
    	self.dicionarioSerasa.registroB002['uf_emissor'].append(self.extrairCampo( 112, bloco, 2))
    	self.dicionarioSerasa.registroB002['filler_2'].append(self.extrairCampo(114, bloco, 1))
    	self.dicionarioSerasa.registroB002['qtd_registros'] = contador
            
    def popularB003(self,bloco, contador):
    
    	self.dicionarioSerasa.registroB003['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
    	self.dicionarioSerasa.registroB003['estado_civil'].append(self.extrairCampo( 4, bloco, 12))
    	self.dicionarioSerasa.registroB003['depend'].append(self.extrairCampo( 16, bloco, 2))
    	self.dicionarioSerasa.registroB003['escolar'].append(self.extrairCampo(18, bloco, 12))
    	self.dicionarioSerasa.registroB003['mun_nasc'].append(self.extrairCampo( 30, bloco, 25))
    	self.dicionarioSerasa.registroB003['uf_nasc'].append(self.extrairCampo(55, bloco, 2))
    	self.dicionarioSerasa.registroB003['cpf_conjuge'].append(self.extrairCampo( 57, bloco, 11))
    	self.dicionarioSerasa.registroB003['ddd_res'].append(self.extrairCampo(68, bloco, 3))
    	self.dicionarioSerasa.registroB003['fone_res'].append(self.extrairCampo( 71, bloco, 9))
    	self.dicionarioSerasa.registroB003['ddd_coml'].append(self.extrairCampo( 80, bloco, 3))
    	self.dicionarioSerasa.registroB003['fone_coml'].append(self.extrairCampo( 83, bloco, 9))
    	self.dicionarioSerasa.registroB003['ramal'].append(self.extrairCampo( 92, bloco, 4))
    	self.dicionarioSerasa.registroB003['celular'].append(self.extrairCampo(96, bloco, 9))
    	self.dicionarioSerasa.registroB003['ddd_cel'].append(self.extrairCampo( 105, bloco, 3))
    	self.dicionarioSerasa.registroB003['filler'].append(self.extrairCampo(108, bloco, 7))
    	self.dicionarioSerasa.registroB003['qtd_registros'] = contador
            
    def popularB004(self,bloco, contador):
    
    	self.dicionarioSerasa.registroB004['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
    	self.dicionarioSerasa.registroB004['logradouro'].append(self.extrairCampo( 4, bloco, 30))
    	self.dicionarioSerasa.registroB004['numero'].append(self.extrairCampo( 34, bloco, 5))
    	self.dicionarioSerasa.registroB004['complemento'].append(self.extrairCampo( 39, bloco, 10))
    	self.dicionarioSerasa.registroB004['bairro'].append(self.extrairCampo( 49, bloco, 20))
    	self.dicionarioSerasa.registroB004['cidade'].append(self.extrairCampo( 69, bloco, 25))
    	self.dicionarioSerasa.registroB004['uf'].append(self.extrairCampo( 94, bloco, 2))
    	self.dicionarioSerasa.registroB004['cep'].append(self.extrairCampo( 96, bloco, 8))
    	self.dicionarioSerasa.registroB004['desde'].append(self.extrairCampo( 104, bloco, 6))
    	self.dicionarioSerasa.registroB004['filler'].append(self.extrairCampo(110, bloco, 5))
    	self.dicionarioSerasa.registroB004['qtd_registros'] = contador
            
    def popularB005(self, bloco, contador):	
			
        self.dicionarioSerasa.registroB005['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB005['ocupacao'].append(self.extrairCampo(4, bloco, 10))
        self.dicionarioSerasa.registroB005['renda'].append(self.extrairCampo(14, bloco, 9))
        self.dicionarioSerasa.registroB005['ct_serie'].append(self.extrairCampo(23, bloco, 5))
        self.dicionarioSerasa.registroB005['num_ct'].append(self.extrairCampo(28, bloco, 7))
        self.dicionarioSerasa.registroB005['filler_1'].append(self.extrairCampo(35, bloco, 1))
        self.dicionarioSerasa.registroB005['partic'].append(self.extrairCampo(36, bloco, 3))
        self.dicionarioSerasa.registroB005['filler_2'].append(self.extrairCampo(39, bloco, 76))
        self.dicionarioSerasa.registroB005['qtd_registros'] = contador

			
    def popularB006(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB006['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB006['empresa'].append(self.extrairCampo(4, bloco, 40))
        self.dicionarioSerasa.registroB006['desde'].append(self.extrairCampo(44, bloco, 6))
        self.dicionarioSerasa.registroB006['tipo_moeda'].append(self.extrairCampo(50, bloco, 3))
        self.dicionarioSerasa.registroB006['profissão'].append(self.extrairCampo(53, bloco, 30))
        self.dicionarioSerasa.registroB006['cargo'].append(self.extrairCampo(83, bloco, 20))
        self.dicionarioSerasa.registroB006['filler'].append(self.extrairCampo(103, bloco, 12))
        self.dicionarioSerasa.registroB006['qtd_registros'] = contador
    			
    			
    def popularB352(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB352['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB352['empresa'].append(self.extrairCampo(4, bloco, 40))
        self.dicionarioSerasa.registroB352['cnpj'].append(self.extrairCampo(44, bloco, 8))
        self.dicionarioSerasa.registroB352['partic'].append(self.extrairCampo(52, bloco, 5))
        self.dicionarioSerasa.registroB352['estado'].append(self.extrairCampo(57, bloco, 2))
        self.dicionarioSerasa.registroB352['situacao'].append(self.extrairCampo(59, bloco, 43))
        self.dicionarioSerasa.registroB352['dt_ini_partic'].append(self.mascaraData(self.extrairCampo(102, bloco, 6)))
        self.dicionarioSerasa.registroB352['dt_ult_atu'].append(self.mascaraData(self.extrairCampo(108, bloco, 6)))
        self.dicionarioSerasa.registroB352['filler'].append(self.extrairCampo(114, bloco, 1))
        self.dicionarioSerasa.registroB352['qtd_registros'] = contador
    			
    			
    def popularB353(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB353['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB353['qtde_total_credito'].append(self.extrairCampo(4, bloco, 3))
        self.dicionarioSerasa.registroB353['data_atual'].append(self.mascaraData(self.extrairCampo(7, bloco, 6)))
        self.dicionarioSerasa.registroB353['qtde_atual'].append(self.extrairCampo(13, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_credito_1'].append(self.extrairCampo(16, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_credito_2'].append(self.extrairCampo(19, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_credito_3'].append(self.extrairCampo(22, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_total_cheque'].append(self.extrairCampo(25, bloco, 3))
        self.dicionarioSerasa.registroB353['qtdeatual'].append(self.extrairCampo(28, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_cheque_1'].append(self.extrairCampo(31, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_cheque_2'].append(self.extrairCampo(34, bloco, 3))
        self.dicionarioSerasa.registroB353['qtde_mes_cheque_3'].append(self.extrairCampo(37, bloco, 3))
        self.dicionarioSerasa.registroB353['posicao'].append(self.extrairCampo(40, bloco, 8))
        self.dicionarioSerasa.registroB353['data_ficad'].append(self.mascaraData(self.extrairCampo(48, bloco, 8)))
        self.dicionarioSerasa.registroB353['filler'].append(self.extrairCampo(56, bloco, 59))
        self.dicionarioSerasa.registroB353['qtd_registros'] = contador
    			
    def popularB354(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB354['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB354['dt_ocorr'].append(self.mascaraData(self.extrairCampo(4, bloco, 8)))
        self.dicionarioSerasa.registroB354['origem'].append(self.extrairCampo(12, bloco, 40))
        self.dicionarioSerasa.registroB354['filler'].append(self.extrairCampo(52, bloco, 63))
        self.dicionarioSerasa.registroB354['qtd_registros'] = contador
    			
    			
    def popularB357(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB357['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB357['qtde_total'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB357['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB357['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB357['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB357['tip_moeda'].append(self.extrairCampo(49, bloco, 3))
        self.dicionarioSerasa.registroB357['vlr_ultima'].append(self.mascaraDinheiro(self.extrairCampo(52, bloco, 9)))
        self.dicionarioSerasa.registroB357['origem'].append(self.extrairCampo(61, bloco, 20))
        self.dicionarioSerasa.registroB357['filial'].append(self.extrairCampo(81, bloco, 4))
        self.dicionarioSerasa.registroB357['tip_pefin'].append(self.extrairCampo(85, bloco, 2))
        self.dicionarioSerasa.registroB357['val_total'].append(self.extrairCampo(87, bloco, 9))
        self.dicionarioSerasa.registroB357['filler'].append(self.extrairCampo(96, bloco, 19))
        self.dicionarioSerasa.registroB357['qtd_registros'] = contador
    			
    def popularB358(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB358['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB358['tipo_pefin'].append(self.extrairCampo(4, bloco, 2))
        self.dicionarioSerasa.registroB358['modalidade'].append(self.extrairCampo(6, bloco, 12))
        self.dicionarioSerasa.registroB358['tipo_ocor'].append(self.extrairCampo(18, bloco, 1))
        self.dicionarioSerasa.registroB358['chave_cadus'].append(self.extrairCampo(19, bloco, 10))
        self.dicionarioSerasa.registroB358['filler_1'].append(self.extrairCampo(29, bloco, 2))
        self.dicionarioSerasa.registroB358['dt_ocorr'].append(self.mascaraData(self.extrairCampo(31, bloco, 8)))
        self.dicionarioSerasa.registroB358['sigla_mod'].append(self.extrairCampo(39, bloco, 2))
        self.dicionarioSerasa.registroB358['principal'].append(self.extrairCampo(41, bloco, 1))
        self.dicionarioSerasa.registroB358['tipo_moeda'].append(self.extrairCampo(42, bloco, 3))
        self.dicionarioSerasa.registroB358['valor'].append(self.mascaraDinheiro(self.extrairCampo(45, bloco, 9)))
        self.dicionarioSerasa.registroB358['contrato'].append(self.extrairCampo(54, bloco, 17))
        self.dicionarioSerasa.registroB358['origem'].append(self.extrairCampo(71, bloco, 20))
        self.dicionarioSerasa.registroB358['filial'].append(self.extrairCampo(91, bloco, 4))
        self.dicionarioSerasa.registroB358['qtde_ocorr'].append(self.extrairCampo(95, bloco, 5))
        self.dicionarioSerasa.registroB358['cod_banco'].append(self.extrairCampo(100, bloco, 4))
        self.dicionarioSerasa.registroB358['subjud_x'].append(self.extrairCampo(104, bloco, 1))
        self.dicionarioSerasa.registroB358['uf'].append(self.extrairCampo(105, bloco, 2))
        self.dicionarioSerasa.registroB358['filler_2'].append(self.extrairCampo(107, bloco, 4))
        self.dicionarioSerasa.registroB358['cont_contrato'].append(self.extrairCampo(111, bloco, 3))
        self.dicionarioSerasa.registroB358['filler_3'].append(self.extrairCampo(114, bloco, 1))
        self.dicionarioSerasa.registroB358['qtd_registros'] = contador
    			
    def popularB359(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB359['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB359['qtde_total'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB359['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB359['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB359['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB359['tip_moeda'].append(self.extrairCampo(49, bloco, 3))
        self.dicionarioSerasa.registroB359['vlr_ultima'].append(self.mascaraDinheiro(self.extrairCampo(52, bloco, 9)))
        self.dicionarioSerasa.registroB359['origem'].append(self.extrairCampo(61, bloco, 20))
        self.dicionarioSerasa.registroB359['filial'].append(self.extrairCampo(81, bloco, 4))
        self.dicionarioSerasa.registroB359['filler'].append(self.extrairCampo(85, bloco, 30))
        self.dicionarioSerasa.registroB359['qtd_registros'] = contador
    			
    def popularB360(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB360['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB360['dt_ocorr'].append(self.mascaraData(self.extrairCampo(4, bloco, 8)))
        self.dicionarioSerasa.registroB360['num_cheque'].append(self.extrairCampo(12, bloco, 6))
        self.dicionarioSerasa.registroB360['alinea'].append(self.extrairCampo(18, bloco, 2))
        self.dicionarioSerasa.registroB360['qtde_cheque'].append(self.extrairCampo(20, bloco, 4))
        self.dicionarioSerasa.registroB360['tipo_moeda'].append(self.extrairCampo(24, bloco, 3))
        self.dicionarioSerasa.registroB360['valor'].append(self.mascaraDinheiro(self.extrairCampo(27, bloco, 9)))
        self.dicionarioSerasa.registroB360['banco'].append(self.extrairCampo(36, bloco, 30))
        self.dicionarioSerasa.registroB360['agencia'].append(self.extrairCampo(66, bloco, 4))
        self.dicionarioSerasa.registroB360['cidade'].append(self.extrairCampo(70, bloco, 25))
        self.dicionarioSerasa.registroB360['uf'].append(self.extrairCampo(95, bloco, 2))
        self.dicionarioSerasa.registroB360['qtde_ocorr'].append(self.extrairCampo(97, bloco, 5))
        self.dicionarioSerasa.registroB360['tipo_conta'].append(self.extrairCampo(102, bloco, 1))
        self.dicionarioSerasa.registroB360['conta'].append(self.extrairCampo(103, bloco, 9))
        self.dicionarioSerasa.registroB360['filler'].append(self.extrairCampo(112, bloco, 3))
        self.dicionarioSerasa.registroB360['qtd_registros'] = contador
    			
    def popularB361(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB361['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB361['qtde_total'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB361['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB361['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB361['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB361['tip_moeda'].append(self.extrairCampo(49, bloco, 3))
        self.dicionarioSerasa.registroB361['valor'].append(self.mascaraDinheiro(self.extrairCampo(52, bloco, 9)))
        self.dicionarioSerasa.registroB361['cidade'].append(self.extrairCampo(61, bloco, 25))
        self.dicionarioSerasa.registroB361['uf'].append(self.extrairCampo(86, bloco, 2))
        self.dicionarioSerasa.registroB361['val_total'].append(self.extrairCampo(88, bloco, 9))
        self.dicionarioSerasa.registroB361['filler'].append(self.extrairCampo(97, bloco, 18))
        self.dicionarioSerasa.registroB361['qtd_registros'] = contador
    			
    def popularB362(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB362['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB362['dt_ocorr'].append(self.mascaraData(self.extrairCampo(4, bloco, 8)))
        self.dicionarioSerasa.registroB362['tipo_moeda'].append(self.extrairCampo(12, bloco, 3))
        self.dicionarioSerasa.registroB362['valor'].append(self.mascaraDinheiro(self.extrairCampo(15, bloco, 9)))
        self.dicionarioSerasa.registroB362['cartorio'].append(self.extrairCampo(24, bloco, 4))
        self.dicionarioSerasa.registroB362['cidade'].append(self.extrairCampo(28, bloco, 25))
        self.dicionarioSerasa.registroB362['uf'].append(self.extrairCampo(53, bloco, 2))
        self.dicionarioSerasa.registroB362['qtde_ocorr'].append(self.extrairCampo(55, bloco, 5))
        self.dicionarioSerasa.registroB362['subjudice'].append(self.extrairCampo(60, bloco, 1))
        self.dicionarioSerasa.registroB362['data'].append(self.mascaraData(self.extrairCampo(61, bloco, 8)))
        self.dicionarioSerasa.registroB362['filler'].append(self.extrairCampo(69, bloco, 46))
        self.dicionarioSerasa.registroB362['qtd_registros'] = contador
    			
    def popularB363(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB363['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB363['qtde_total'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB363['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB363['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB363['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB363['tip_moeda'].append(self.extrairCampo(49, bloco, 3))
        self.dicionarioSerasa.registroB363['valor'].append(self.mascaraDinheiro(self.extrairCampo(52, bloco, 9)))
        self.dicionarioSerasa.registroB363['natureza'].append(self.extrairCampo(61, bloco, 20))
        self.dicionarioSerasa.registroB363['uf'].append(self.extrairCampo(81, bloco, 2))
        self.dicionarioSerasa.registroB363['val_total'].append(self.extrairCampo(83, bloco, 9))
        self.dicionarioSerasa.registroB363['filler'].append(self.extrairCampo(92, bloco, 23))
        self.dicionarioSerasa.registroB363['qtd_registros'] = contador
    			
    def popularB364(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB364['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB364['dt_ocorr'].append(self.mascaraData(self.extrairCampo(4, bloco, 8)))
        self.dicionarioSerasa.registroB364['natureza'].append(self.extrairCampo(12, bloco, 20))
        self.dicionarioSerasa.registroB364['principal'].append(self.extrairCampo(32, bloco, 1))
        self.dicionarioSerasa.registroB364['tipo_moeda'].append(self.extrairCampo(33, bloco, 3))
        self.dicionarioSerasa.registroB364['valor'].append(self.mascaraDinheiro(self.extrairCampo(36, bloco, 9)))
        self.dicionarioSerasa.registroB364['distribuid'].append(self.extrairCampo(45, bloco, 4))
        self.dicionarioSerasa.registroB364['vara'].append(self.extrairCampo(49, bloco, 4))
        self.dicionarioSerasa.registroB364['cidade'].append(self.extrairCampo(53, bloco, 25))
        self.dicionarioSerasa.registroB364['uf'].append(self.extrairCampo(78, bloco, 2))
        self.dicionarioSerasa.registroB364['qtde_ocorr'].append(self.extrairCampo(80, bloco, 5))
        self.dicionarioSerasa.registroB364['subjudice'].append(self.extrairCampo(85, bloco, 1))
        self.dicionarioSerasa.registroB364['filler'].append(self.extrairCampo(86, bloco, 29))
        self.dicionarioSerasa.registroB364['qtd_registros'] = contador
    			
    def popularB365(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB365['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB365['total_ocor'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB365['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB365['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB365['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB365['empresa'].append(self.extrairCampo(49, bloco, 40))
        self.dicionarioSerasa.registroB365['filler'].append(self.extrairCampo(89, bloco, 26))
        self.dicionarioSerasa.registroB365['qtd_registros'] = contador
    			
    def popularB366(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB366['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB366['data_ocor'].append(self.extrairCampo(4, bloco, 8))
        self.dicionarioSerasa.registroB366['tipo_ocor'].append(self.extrairCampo(12, bloco, 10))
        self.dicionarioSerasa.registroB366['cnpj_pie'].append(self.extrairCampo(22, bloco, 14))
        self.dicionarioSerasa.registroB366['empresa'].append(self.extrairCampo(36, bloco, 45))
        self.dicionarioSerasa.registroB366['total_ocor'].append(self.extrairCampo(81, bloco, 5))
        self.dicionarioSerasa.registroB366['qualif'].append(self.extrairCampo(86, bloco, 3))
        self.dicionarioSerasa.registroB366['vara_civil'].append(self.extrairCampo(89, bloco, 4))
        self.dicionarioSerasa.registroB366['filler'].append(self.extrairCampo(93, bloco, 22))
        self.dicionarioSerasa.registroB366['qtd_registros'] = contador
    			
    def popularB367(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB367['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB367['qtde_total'].append(self.extrairCampo(4, bloco, 5))
        self.dicionarioSerasa.registroB367['descricao'].append(self.extrairCampo(9, bloco, 28))
        self.dicionarioSerasa.registroB367['data_menor'].append(self.mascaraData(self.extrairCampo(37, bloco, 6)))
        self.dicionarioSerasa.registroB367['data_maior'].append(self.mascaraData(self.extrairCampo(43, bloco, 6)))
        self.dicionarioSerasa.registroB367['tip_moeda'].append(self.extrairCampo(49, bloco, 3))
        self.dicionarioSerasa.registroB367['vlr_ultima'].append(self.mascaraDinheiro(self.extrairCampo(52, bloco, 9)))
        self.dicionarioSerasa.registroB367['origem'].append(self.extrairCampo(61, bloco, 20))
        self.dicionarioSerasa.registroB367['local'].append(self.extrairCampo(81, bloco, 4))
        self.dicionarioSerasa.registroB367['val_total'].append(self.extrairCampo(85, bloco, 9))
        self.dicionarioSerasa.registroB367['filler'].append(self.extrairCampo(94, bloco, 21))
        self.dicionarioSerasa.registroB367['qtd_registros'] = contador
    			
    def popularB368(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB368['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB368['dt_ocorr'].append(self.mascaraData(self.extrairCampo(4, bloco, 8)))
        self.dicionarioSerasa.registroB368['natureza'].append(self.extrairCampo(12, bloco, 2))
        self.dicionarioSerasa.registroB368['tipo_moeda'].append(self.extrairCampo(14, bloco, 3))
        self.dicionarioSerasa.registroB368['valor'].append(self.mascaraDinheiro(self.extrairCampo(17, bloco, 9)))
        self.dicionarioSerasa.registroB368['titulo'].append(self.extrairCampo(26, bloco, 17))
        self.dicionarioSerasa.registroB368['origem'].append(self.extrairCampo(43, bloco, 20))
        self.dicionarioSerasa.registroB368['local'].append(self.extrairCampo(63, bloco, 4))
        self.dicionarioSerasa.registroB368['qtde_ocorr'].append(self.extrairCampo(67, bloco, 5))
        self.dicionarioSerasa.registroB368['tipo_ocor'].append(self.extrairCampo(72, bloco, 1))
        self.dicionarioSerasa.registroB368['chave_cadus'].append(self.extrairCampo(73, bloco, 10))
        self.dicionarioSerasa.registroB368['filler_1'].append(self.extrairCampo(83, bloco, 15))
        self.dicionarioSerasa.registroB368['nova_natur'].append(self.extrairCampo(98, bloco, 3))
        self.dicionarioSerasa.registroB368['subjudice'].append(self.extrairCampo(101, bloco, 1))
        self.dicionarioSerasa.registroB368['serie_cadus'].append(self.extrairCampo(102, bloco, 1))
        self.dicionarioSerasa.registroB368['filler_2'].append(self.extrairCampo(103, bloco, 12))
        self.dicionarioSerasa.registroB368['qtd_registros'] = contador
    			
    def popularB370(self, bloco, contador):	
    	
        self.dicionarioSerasa.registroB370['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB370['tipo_de_reg'].append(self.extrairCampo(4, bloco, 1))
        self.dicionarioSerasa.registroB370['ddd_do_tel'].append(self.extrairCampo(5, bloco, 3))
        self.dicionarioSerasa.registroB370['nro_fone'].append(self.extrairCampo(8, bloco, 9))
        self.dicionarioSerasa.registroB370['endereco'].append(self.extrairCampo(17, bloco, 70))
        self.dicionarioSerasa.registroB370['bairro'].append(self.extrairCampo(87, bloco, 20))
        self.dicionarioSerasa.registroB370['cep'].append(self.extrairCampo(107, bloco, 8))
        self.dicionarioSerasa.registroB370['qtd_registros'] = contador
        
    			
    def popularB370_TIPO2(self, bloco, contador):	
		
        self.dicionarioSerasa.registroB370_TIPO2['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB370_TIPO2['tipo_de_reg'].append(self.extrairCampo(4, bloco, 1))
        self.dicionarioSerasa.registroB370_TIPO2['cidade'].append(self.extrairCampo(5, bloco, 30))
        self.dicionarioSerasa.registroB370_TIPO2['uf'].append(self.extrairCampo(35, bloco, 2))
        self.dicionarioSerasa.registroB370_TIPO2['nome'].append(self.extrairCampo(37, bloco, 50))
        self.dicionarioSerasa.registroB370_TIPO2['dt_atualiza'].append(self.mascaraData(self.extrairCampo(87, bloco, 8)))
        self.dicionarioSerasa.registroB370_TIPO2['filler'].append(self.extrairCampo(95, bloco, 20))
        self.dicionarioSerasa.registroB370_TIPO2['qtd_registros'] = contador
    			
    			
    def popularB376(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB376['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB376['tipo_de_reg'].append(self.extrairCampo(4, bloco, 1))
        self.dicionarioSerasa.registroB376['tipo_doc'].append(self.extrairCampo(5, bloco, 1))
        self.dicionarioSerasa.registroB376['desc_tp_doc'].append(self.extrairCampo(6, bloco, 6))
        self.dicionarioSerasa.registroB376['nro_doc'].append(self.extrairCampo(12, bloco, 20))
        self.dicionarioSerasa.registroB376['data_ocorr'].append(self.mascaraData(self.extrairCampo(32, bloco, 10)))
        self.dicionarioSerasa.registroB376['motivo_ocorr'].append(self.extrairCampo(42, bloco, 10))
        self.dicionarioSerasa.registroB376['ddd_contato'].append(self.extrairCampo(52, bloco, 3))
        self.dicionarioSerasa.registroB376['fone_contato'].append(self.extrairCampo(55, bloco, 8))
        self.dicionarioSerasa.registroB376['filler'].append(self.extrairCampo(63, bloco, 51))
        self.dicionarioSerasa.registroB376['qtd_registros'] = contador
    			
    def popularB376_CELULAR(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB376_CELULAR['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB376_CELULAR['tipo_de_reg'].append(self.extrairCampo(4, bloco, 1))
        self.dicionarioSerasa.registroB376_CELULAR['tipo_doc'].append(self.extrairCampo(5, bloco, 1))
        self.dicionarioSerasa.registroB376_CELULAR['desc_tp_doc'].append(self.extrairCampo(6, bloco, 6))
        self.dicionarioSerasa.registroB376_CELULAR['nro_doc'].append(self.extrairCampo(12, bloco, 20))
        self.dicionarioSerasa.registroB376_CELULAR['data_ocorr'].append(self.mascaraData(self.extrairCampo(32, bloco, 10)))
        self.dicionarioSerasa.registroB376_CELULAR['motivo_ocorr'].append(self.extrairCampo(42, bloco, 10))
        self.dicionarioSerasa.registroB376_CELULAR['ddd_contato'].append(self.extrairCampo(52, bloco, 3))
        self.dicionarioSerasa.registroB376_CELULAR['fone_contato'].append(self.extrairCampo(55, bloco, 9))
        self.dicionarioSerasa.registroB376_CELULAR['filler'].append(self.extrairCampo(64, bloco, 50))
        self.dicionarioSerasa.registroB376_CELULAR['qtd_registros'] = contador
    			
    def popularB376_TIPO2(self, bloco, contador):	
    			
        self.dicionarioSerasa.registroB376_TIPO2['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroB376_TIPO2['tipo_de_reg'].append(self.extrairCampo(4, bloco, 1))		
        self.dicionarioSerasa.registroB376_TIPO2['mensagem'].append(self.extrairCampo(5, bloco, 65))
        self.dicionarioSerasa.registroB376_TIPO2['mens_reduzida'].append(self.extrairCampo(70, bloco, 32))
        self.dicionarioSerasa.registroB376_TIPO2['filler'].append(self.extrairCampo(102, bloco, 12))
        self.dicionarioSerasa.registroB376_TIPO2['qtd_registros'] = contador
        
    def popularB280(self, bloco, contador):	
    	
        self.dicionarioSerasa.registroB280['tipo_reg'] = self.extrairCampo(0, bloco, 4)
        self.dicionarioSerasa.registroB280['tipo'] = self.extrairCampo(4, bloco, 4)
        self.dicionarioSerasa.registroB280['score'] = self.extrairCampo(8, bloco, 6)
        self.dicionarioSerasa.registroB280['range'] = self.extrairCampo(14, bloco, 6)
        self.dicionarioSerasa.registroB280['taxa'] = self.extrairCampo(20, bloco, 5)
        self.dicionarioSerasa.registroB280['mensagem'] = self.extrairCampo(25, bloco, 49)
        self.dicionarioSerasa.registroB280['codigo'] = self.extrairCampo(74, bloco, 6)
        self.dicionarioSerasa.registroB280['filler'] = self.extrairCampo(80, bloco, 35)
        self.dicionarioSerasa.registroB280['qtd_registros'] = contador
		
    def popularP002(self, bloco, contador):	
    	
        self.dicionarioSerasa.registroP002['tipo_reg'].append(self.extrairCampo(0, bloco, 4))
        self.dicionarioSerasa.registroP002['primeiro_cod'].append(self.extrairCampo(4, bloco, 4))
        self.dicionarioSerasa.registroP002['primeira_chave'].append(self.extrairCampo(8, bloco, 21))
        self.dicionarioSerasa.registroP002['segundo_cod'].append(self.extrairCampo(29, bloco, 4))
        self.dicionarioSerasa.registroP002['segunda_chave'].append(self.extrairCampo(33, bloco, 21))
        self.dicionarioSerasa.registroP002['terceiro_cod'].append(self.extrairCampo(54, bloco, 4))
        self.dicionarioSerasa.registroP002['terceira_chave'].append(self.extrairCampo(58, bloco, 21))
        self.dicionarioSerasa.registroP002['quarto_cod'].append(self.extrairCampo(79, bloco, 4))
        self.dicionarioSerasa.registroP002['quarta_chave'].append(self.extrairCampo(83, bloco, 21))
        self.dicionarioSerasa.registroP002['filler'].append(self.extrairCampo(104, bloco, 11))
        self.dicionarioSerasa.registroP002['qtd_registros'] = contador
    
    #Abaixo Métodos GET para cada bloco e seus items     
    def getRegistroB001(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB001[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB001[item] 
            
    def getRegistroB49C(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB49C[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB49C[item]
            
    def getRegistroB002(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB002[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB002[item]
            
    def getRegistroB003(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB003[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB003[item]
            
    def getRegistroB004(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB004[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB004[item]
            
    def getRegistroB005(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB005[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB005[item]
            
    def getRegistroB006(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB006[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB006[item]
            
    def getRegistroB352(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB352[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB352[item]
            
    def getRegistroB353(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB353[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB353[item]
            
    def getRegistroB354(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB354[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB354[item]
            
    def getRegistroB357(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB357[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB357[item]
            
    def getRegistroB358(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB358[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB358[item]
            
    def getRegistroB359(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB359[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB359[item]

    def getRegistroB360(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB360[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB360[item]
            
    def getRegistroB361(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB361[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB361[item]

    def getRegistroB362(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB362[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB362[item]

    def getRegistroB363(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB363[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB363[item]
            
    def getRegistroB364(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB364[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB364[item]
            
    def getRegistroB365(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB365[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB365[item]
            
    def getRegistroB366(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB366[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB366[item]
            
    def getRegistroB367(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB367[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB367[item]
            
    def getRegistroB368(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB368[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB368[item]

    def getRegistroB370(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB370[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB370[item]
            
    def getRegistroB370_Tipo2(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB370_TIPO2[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB370_TIPO2[item]
        
    def getRegistroB376(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB376[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB376[item]
            
    def getRegistroB376_TIPO2(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB376_TIPO2[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB376_TIPO2[item]
            
    def getRegistroB376_CELULAR(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB376_CELULAR[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB376_CELULAR[item]
            
    def getRegistroA900(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroA900[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroA900[item]
            
    def getRegistroB280(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroB280[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroB280[item]
			
    def getRegistroP002(self, item, pos='', indice=False):
        
        if not self.inicializado:
            self.consulta()
            self.inicializado = True
        
        if indice:
            try:
                aux = self.dicionarioSerasa.registroP002[item][pos]
            except:
                aux = ''
                
            return aux
        else:        
            return self.dicionarioSerasa.registroP002[item]
    
    #Método que verifica os blocos vindos na string de Resposta da Serasa
    def loopBlocos(self, vetor_linha_blocos):
        contadorB002 = 0          
        contadorB003 = 0
        contadorB004 = 0
        contadorB005 = 0
        contadorB006 = 0
        contadorB352 = 0
        contadorB353 = 0
        contadorB354 = 0
        contadorB357 = 0
        contadorB358 = 0
        contadorB359 = 0
        contadorB360 = 0
        contadorB361 = 0
        contadorB362 = 0
        contadorB363 = 0
        contadorB364 = 0
        contadorB365 = 0
        contadorB366 = 0
        contadorB367 = 0
        contadorB368 = 0
        contadorB370 = 0
        contadorB370_TIPO2 = 0
        contadorB376 = 0
        contadorB376_TIPO2 = 0
        contadorB376_CELULAR = 0
        contadorA900 = 0
        contadorP002 = 0
        contadorB280 = 0
        
        lista_respostas = ['B280','P002','P006', 'A900', 'T999', 'B49C','B001','B002','B003','B004','B005','B006','B352','B353','B354','B357','B358','B359','B360','B361','B362','B363','B364','B365','B366','B367','B368','B370','B376']
        
        for bloco in vetor_linha_blocos:            
            if bloco[0:4]=="B49C":
                self.popularB49C(bloco)
            if bloco[0:4]=="B001":
                self.popularB001(bloco)  
            if bloco[0:4]=="B002":
                contadorB002 +=1
                self.popularB002(bloco, contadorB002)  
            if bloco[0:4]=="B003":
                contadorB003 += 1
                self.popularB003(bloco, contadorB003)  
            if bloco[0:4]=="B004":
                contadorB004 += 1
                self.popularB004(bloco, contadorB004)  
            if bloco[0:4]=="B005":
                contadorB005 += 1
                self.popularB005(bloco, contadorB005)  
            if bloco[0:4]=="B006":
                contadorB006 += 1
                self.popularB006(bloco, contadorB006)  
            if bloco[0:4]=="B352":
                contadorB352 += 1
                self.popularB352(bloco, contadorB352)  
            if bloco[0:4]=="B353":
                contadorB353 += 1
                self.popularB353(bloco, contadorB353)  
            if bloco[0:4]=="B354":
                contadorB354 += 1
                self.popularB354(bloco, contadorB354)  
            if bloco[0:4]=="B357":
                contadorB357 += 1
                self.popularB357(bloco, contadorB357)  
            if bloco[0:4]=="B358":
                contadorB358 += 1
                self.popularB358(bloco, contadorB358)  
            if bloco[0:4]=="B359":
                contadorB359 += 1
                self.popularB359(bloco, contadorB359)
            if bloco[0:4]=="B360":
                contadorB360 += 1
                self.popularB360(bloco, contadorB360)  
            if bloco[0:4]=="B361":
                contadorB361 += 1
                self.popularB361(bloco, contadorB361)  
            if bloco[0:4]=="B362":
                contadorB362 += 1
                self.popularB362(bloco, contadorB362)  
            if bloco[0:4]=="B363":
                contadorB363 += 1
                self.popularB363(bloco, contadorB363)  
            if bloco[0:4]=="B364":
                contadorB364 += 1
                self.popularB364(bloco, contadorB364)  
            if bloco[0:4]=="B365":
                contadorB365 += 1
                self.popularB365(bloco, contadorB365)  
            if bloco[0:4]=="B366":
                contadorB366 += 1
                self.popularB366(bloco, contadorB366)  
            if bloco[0:4]=="B367":
                contadorB367 += 1
                self.popularB367(bloco, contadorB367)  
            if bloco[0:4]=="B368":
                contadorB368 += 1
                self.popularB368(bloco, contadorB368) 
                
            if bloco[0:4]=="B280":
                contadorB280 += 1
                self.popularB280(bloco, contadorB280)
            if bloco[0:4]=="P002":
                contadorP002 += 1
                self.popularP002(bloco, contadorP002)
            
            if bloco[0:4]=="B370":
                if bloco[0:5]=="B3702":
                    contadorB370_TIPO2 += 1
                    self.popularB370_TIPO2(bloco, contadorB370_TIPO2)
                else:
                    contadorB370 +=1
                    self.popularB370(bloco, contadorB370)
            
            if bloco[0:5]=="B3762":
                contadorB376_TIPO2 += 1
                self.popularB376_TIPO2(bloco, contadorB376_TIPO2)
            if bloco[0:4]=="B376" and bloco[64:1]==" ":
                contadorB376 += 1
                self.popularB376(bloco, contadorB376)
            if bloco[0:4]=="B376" and bloco[64:1]!=" ": 
                contadorB376_CELULAR += 1
                self.popularB376_CELULAR(bloco, contadorB376_CELULAR)
            	
            if bloco[0:4]=="A900":
                contadorA900 += 1
                self.popularA900(bloco, contadorA900)
                
            if  bloco[0:4] not in lista_respostas:
                self.blocos_nao_listados.append(bloco[0:4])
                
    #Método que consulta e adiciona dados para o status de execução da consulta Serasa            
    def consulta_status_execucao(self, status_execucao = 0,  descricao_status_execucao = 'CONSULTA REALIZADA COM SUCESSO'):
        datahora = datetime.datetime.now()
        datahora = datahora.strftime('%Y-%m-%d %H:%M')
        
        self.dicionarioSerasa.dicionario_status_execucao = {
            'id' : str(self.file_name),
            'status_execucao' : status_execucao, 
            'descricao_status_execucao' : descricao_status_execucao,
            'datahora' : datahora,
            'blocos_nao_listados' : self.blocos_nao_listados}
        
        return self.dicionarioSerasa.dicionario_status_execucao             
    
    #Método que monta a string de consulta Serasa            
    def gerar_string_envio(self, documento_consultado,
                           tipo_pessoa_busca,  uf_cliente,
                           login, senha):
        
        feature = 'P006'
        dados = 'p=' \
                + login + senha + '        B49C      '+'0000' + documento_consultado +\
                tipo_pessoa_busca + 'C     FI0000000            S99SINIAN    ' \
                                    '                                     N  ' \
                                    'S                                       ' \
                                    '     ' \
                + '              ' + '                                    ' \
                                        '                                    ' \
                                        '                                    ' \
                                        '                                    ' \
                                        '                                    ' \
                                        '                                    ' \
                                        '                   ' + self.string_consulta + feature +'NNSSS  05SSSSNSSS NNNNSN NN                                                                                    T999' \
        
        return dados
    
    #Método que faz a consulta a Serasa
    def realizar_busca_serasa(self, dados):
        endpoint = 'https://sitenet43-2.serasa.com.br/Prod/consultahttps?' #PRODUÇÃO
        #endpoint = 'https://mqlinuxext.serasa.com.br/Homologa/consultahttps?' #HOMOLOGAÇÃO
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        request = requests.post(endpoint, data=dados, headers=headers)
        
        return request.text
    
    #Método que executa as consultas por bloco a partir do string formado para consulta Serasa    
    def executarConsulta(self, string_dados_retorno):
        
        string_dados_retorno = string_dados_retorno[
                               0:len(string_dados_retorno)-2] + 'T999'
        
        string_dados_retorno = re.findall(".+?(?=T999)", string_dados_retorno)
        tamanho = 400
        string_dados_retorno = string_dados_retorno[0]
        bloco_b49c = string_dados_retorno[:tamanho]
        
        resto = string_dados_retorno[tamanho:]
        
        qtd_resposta = len(resto) // 115
        
        x = resto
        chunks, chunk_size = len(resto), len(resto) // qtd_resposta

        vetor_linha_blocos =[ x[i:i+chunk_size] for i in range(0, chunks, chunk_size) ]        
        vetor_linha_blocos.append(bloco_b49c)

        self.loopBlocos(vetor_linha_blocos)                
                    
    def extrairCampo(self, inicio, bloco, tamanho):        
        return bloco[inicio:inicio+tamanho].strip()
    
    #métodos para mascaramento de campos de valor e data
    def mascaraData(self, valor):

        if len(valor) < 8:
            valor = valor[0:4] + "-" + valor[4:6]
        else:
            valor = valor[0:4] + "-" + valor[4:6] + "-" + valor[6:8]

        return valor
        
    def mascaraDinheiro(self, valor):

        valor = valor[0:len(valor)-2] + "." + valor[len(valor)-2:len(valor)]
        valor = float(valor)

        return valor
    
    #Método que lista outras features para serem adicionadas a consulta
    def setFeature(self, features): 
        self.feature.append(features)
    
    #método que adiciona string para outras consultas P002
    def configuraFeature(self, features): 

        chave_1 = ''
        chave_2 = ''
        chave_3 = ''
        chave_4 = ''
        feature_1 = ''
        feature_2 = ''
        feature_3 = ''
        feature_4 = ''
        novo_score = ' ' * 17
        antigo_score = ' ' * 21
        feat_vazia = ' ' * 4

        if  0 <= len(features)-1 and features[0] == 'BPHPHSPN':
            feature_1 = features[0]
            chave_1 = novo_score
        elif 0 <= len(features)-1:
            feature_1 = features[0]
            chave_1 = antigo_score
            
        if  1 <= len(features)-1 and features[1] == 'BPHPHSPN':
            feature_2 = features[1]
            chave_2 = novo_score
        elif 1 <= len(features)-1:
            feature_2 = features[1]
            chave_2 = antigo_score
        else:
            feature_2 = feat_vazia
            chave_2 = antigo_score
            
        if  2 <= len(features) -1  and features[2] == 'BPHPHSPN':
            feature_3 = features[2]
            chave_3 = novo_score
        elif 2 <= len(features) -1:
            feature_3 = features[2]
            chave_3 = antigo_score
        else:
            feature_3 = feat_vazia
            chave_3 = antigo_score
            
        if 3 <= len(features) -1  and features[3] == 'BPHPHSPN':
            feature_4 = features[3]
            chave_4 = novo_score
        elif 3 <= len(features) -1:
            feature_4 = features[3]
            chave_4 = antigo_score
        else:
            feature_4 = feat_vazia
            chave_4 = antigo_score
        #Atribuição dos campos da string para outras consultas com suas features e chaves    
        self.string_consulta = f'P002{feature_1}{chave_1}{feature_2}{chave_2}{feature_3}{chave_3}{feature_4}{chave_4}           '
        
    # Faz upload de arquivos para o S3  
    def upload_raw_to_s3(self, bucket, string):
        s3 = boto3.resource('s3')
        self.buff.write(string.encode())
        try:
            s3.Object(bucket, str(self.teste)).put(Body=self.buff.getvalue())
            return True
        except ClientError:
            return False
    
        # Faz upload de arquivos para o S3  
    def upload_processed_to_s3(self, bucket):
        
        teste = ''
        dicionarios = [
                self.dicionarioSerasa.dicionario_status_execucao,
                self.dicionarioSerasa.registroB49C,
                self.dicionarioSerasa.registroB001,
                self.dicionarioSerasa.registroB001_outros,
                self.dicionarioSerasa.registroA900,
                self.dicionarioSerasa.registroB002,
                self.dicionarioSerasa.registroB003,
                self.dicionarioSerasa.registroB004,
                self.dicionarioSerasa.registroB005,
                self.dicionarioSerasa.registroB006,
                self.dicionarioSerasa.registroB352,
                self.dicionarioSerasa.registroB35P,
                self.dicionarioSerasa.registroB353,
                self.dicionarioSerasa.registroB354,
                self.dicionarioSerasa.registroB357,
                self.dicionarioSerasa.registroB358,
                self.dicionarioSerasa.registroB359,
                self.dicionarioSerasa.registroB360,
                self.dicionarioSerasa.registroB361,
                self.dicionarioSerasa.registroB362,
                self.dicionarioSerasa.registroB363,
                self.dicionarioSerasa.registroB364,
                self.dicionarioSerasa.registroB365,
                self.dicionarioSerasa.registroB366,
                self.dicionarioSerasa.registroB367,
                self.dicionarioSerasa.registroB368,
                self.dicionarioSerasa.registroB370,
                self.dicionarioSerasa.registroB370_TIPO2,
                self.dicionarioSerasa.registroB376,
                self.dicionarioSerasa.registroB376_CELULAR,
                self.dicionarioSerasa.registroB376_TIPO2,
                self.dicionarioSerasa.registroB280,
                self.dicionarioSerasa.registroP002 ]
        
        for item in range(len(dicionarios)):
            teste += pprint.pformat(dicionarios[item])
            # self.buff_processed.write(teste + '/n')
        
        print(teste)
        user_encode_data = json.dumps(teste).encode('utf-8')

        self.buff_processed.write(user_encode_data)
        s3 = boto3.resource('s3')    
        try:
            s3.Object(bucket, str(self.teste)+'processed.txt').put(Body=self.buff_processed.getvalue())
            return True
        except ClientError:
            return False
    
    #Método que faz as consultas no serasa a partir dos parâmetros enviados
    def consulta(self):
        
        #Direciona os CPFs recebidos oara consulta
        cpf = self.input_file_values['cpf']
        cpf = str(cpf).zfill(11)
        
        #Valida se existe alguma feature adicional de consulta
        if 1 <= len(self.feature):
            self.configuraFeature(self.feature)
        
        #Validação de tentativas de execução para direcionar erro em caso de não sucesso sem falha.
        retries = 0
        while retries < 3:
            try:
                consultar = self.gerar_string_envio( cpf,
                                   'F', 'P006',
                                   '85444333', '#3mniEng')
                string = self.realizar_busca_serasa(consultar)
                self.executarConsulta(string)
                # file = StringIO(string)
                self.consulta_status_execucao()
                self.upload_raw_to_s3(self.bucket, string)
                # s3 = boto3.resource('s3')
                # self.buff.write(string.encode())
                # s3.Object('modelagem', str(self.file_name)).put(Body=self.buff.getvalue())
                self.upload_processed_to_s3(self.bucket)
                print(self.consulta_status_execucao())
                retries = 3
            except requests.exceptions.RequestException as e:  
                print(f'O servidor de consulta Serasa está fora do ar. - {retries} ', e)
                time.sleep(4)
                retries += 1
                if retries == 2:
                    erro = self.consulta_status_execucao(1, 'ERRO AO CONSULTAR SERASA')
                    print(erro)