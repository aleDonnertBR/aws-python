import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_timestamp, to_date, date_format, col
from awsglue.dynamicframe import DynamicFrame


#Codigo que faz a carga na tabela BOTICARIO_SENTIMENTOS_TWITTER do postgres.

#declaração de argumento para execução de job GLUE-AWS com código Python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

#Criação de sparkcontext para execução spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#gera dynamic dataframe para carga de dados obtidos via script de extração de dados CSV enviados
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "boticaread_boticario", transformation_ctx = "datasource0")

#mapeamento de colunas origem e destino para ETL via GLUE Python
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("id_marca", "long", "id_marca", "int"), ("marca", "string", "marca", "string"), ("id_linha", "long", "id_linha", "int"), ("linha", "string", "linha", "string"), ("data_venda", "string", "data_venda", "string"), ("qtd_venda", "long", "qtd_venda", "int")], transformation_ctx = "applymapping1")

#Seleção de campos para ETL 
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["marca", "linha", "qtd_venda", "data_venda", "id_marca", "id_linha"], transformation_ctx = "selectfields2")

#Resolvendo parse entre dados de origem e dados catalogados no Catalog Glue
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "boticario", table_name = "postgres_public_vendas_boticario", transformation_ctx = "resolvechoice3")

#Transformando em colunas dados pós parse
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

#Realiza carga dos dados parseados no database boticario e na tabela apontada a partir do catalogo do GLUE
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "boticario", table_name = "postgres_public_vendas_boticario", transformation_ctx = "datasink5")
job.commit()