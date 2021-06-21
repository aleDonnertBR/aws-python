spark

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql import *


glueContext = GlueContext(SparkContext.getOrCreate())
vendas_periodo_DyF = glueContext.create_dynamic_frame.from_catalog(database="boticario", table_name="postgres_public_vendas_boticario_periodo")


df = vendas_periodo_DyF.toDF()
df.createOrReplaceTempView('vendas_periodo')
df = spark.sql(""" select * from vendas_periodo """)

df.show()

glueContext = GlueContext(SparkContext.getOrCreate())
vendas_marca_linha_DyF = glueContext.create_dynamic_frame.from_catalog(database="boticario", table_name="postgres_public_vendas_boticario_marca_linha")


df = vendas_marca_linha_DyF.toDF()
df.createOrReplaceTempView('vendas_marca_linha')
df = spark.sql(""" select * from vendas_marca_linha """)

df.show()

glueContext = GlueContext(SparkContext.getOrCreate())
vendas_marca_periodo_DyF = glueContext.create_dynamic_frame.from_catalog(database="boticario", table_name="postgres_public_vendas_boticario_marca_periodo")


df = vendas_marca_periodo_DyF.toDF()
df.createOrReplaceTempView('vendas_marca_periodo')
df = spark.sql(""" select * from vendas_marca_periodo """)

df.show()

glueContext = GlueContext(SparkContext.getOrCreate())
vendas_linha_periodo_DyF = glueContext.create_dynamic_frame.from_catalog(database="boticario", table_name="postgres_public_vendas_boticario_linha_periodo")


df = vendas_linha_periodo_DyF.toDF()
df.createOrReplaceTempView('vendas_linha_periodo')
df = spark.sql(""" select * from vendas_linha_periodo """)

df.show()


