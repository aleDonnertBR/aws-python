import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from awsglue.dynamicframe import DynamicFrame

#Codigo que faz a carga na tabela VENDAS_BOTICARIO_MARCA_LINHA do postgres.

#declaração de argumento para execução de job GLUE-AWS com código Python
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
#Criação de sparkcontext para execução spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#gera dynamic dataframe para carga de dados obtidos via tabela com dados do CSV
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "boticario", table_name = "postgres_public_vendas_boticario", transformation_ctx = "datasource0")

#transformo o dynamic dataframe para dataframe conseguindo assim gerar a query de consolidacao dos dados
df = datasource0.toDF()
df.createOrReplaceTempView('glue_table')
df = spark.sql("""
select MARCA, 
LINHA,
cast(sum(qtd_venda) as int) as qtd_venda
from glue_table
group by MARCA,LINHA
order by 1,2
""")
#retorno para dynamic dataframe do AWS glue os dados consolidados no dataframe para posterior carga na tabela a partir dos proximos passos
dfy = DynamicFrame.fromDF(df, glueContext, "dfy")

#mapeamento de colunas origem e destino para ETL via GLUE Python
applymapping1 = ApplyMapping.apply(frame = dfy, mappings = [("marca", "string", "marca", "string"), ("linha", "string", "linha", "string"), ("qtd_venda", "int", "qtd_venda", "int")], transformation_ctx = "applymapping1")

#Seleção de campos para ETL 
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["marca", "linha", "qtd_venda"], transformation_ctx = "selectfields2")

#Resolvendo parse entre dados de origem e dados catalogados no Catalog Glue
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "boticario", table_name = "postgres_public_vendas_boticario_marca_linha", transformation_ctx = "resolvechoice3")

#Transformando em colunas dados pós parse
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

#Realiza carga dos dados parseados no database boticario e na tabela apontada a partir do catalogo do GLUE
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "boticario", table_name = "postgres_public_vendas_boticario_marca_linha", transformation_ctx = "datasink5")
job.commit()