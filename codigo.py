from pyspark.sql import SparkSession

import pandas as pd

import numpy as np

import matplotlib.pyplot as plt

from pyspark.sql.functions import col

from pyspark.sql.window import Window


# LOCAL QUE ESTÃO OS DADOS BRUTOS
storage_account_name = "blobs333"
storage_account_access_key = ""
container_name = "csv"


# STORAGE 'FAKE' NO DATABRICKS
storage_account_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
dbutils.fs.mount(
  source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point="/mnt/ana-monteiro",
  extra_configs={storage_account_key: storage_account_access_key}
)

#MOSTRA OS CSV IMPORTADOS DO BLOB STORAGE
#display(dbutils.fs.ls("/mnt/ana-rox"))


# INICIALIZAR SPARK
spark = SparkSession.builder.appName("Carregando Dados do Azure Blob").getOrCreate()

# LER E MOSTRAR CSV EM DATAFRAMES
df_Person = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Person.Person.csv")
#display(df_Person)

df_Product = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Production.Product.csv")
#display(df_Product)

df_Sales_Customer = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Sales.Customer.csv")
#display(df_Sales_Customer)

df_Sales_Detail = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Sales.SalesOrderDetail.csv")
#display(df_Sales_Detail)

df_Sales_Header = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Sales.SalesOrderHeader.csv")
#display(df_Sales_Header)

df_Special_Product = spark.read.option("header", "true").option("inferSchema", True).option("delimiter", ";").csv("/mnt/ana-monteiro/Sales.SpecialOfferProduct.csv")
#display(df_Special_Product)


# TÓPICO 1
df_Sales_Detail_filtrado = df_Sales_Detail.groupBy("SalesOrderID").count().filter(col("count") >= 3)
df_Sales_Detail_filtrado.show()


# TÓPICO 2
df_juncao = df_Sales_Detail.join(df_Special_Product, "SpecialOfferID") \
                           .join(df_Product, "ProductID")

# CALCULAR A SOMA DE ORDERQTY POR NOME DO PRODUTO E DAYSTOMANUFACTURE
df_soma_g = df_juncao.groupBy("Name", "DaysToManufacture") \
                      .agg({"OrderQty": "sum"}) \
                      .withColumnRenamed("sum(OrderQty)", "TotalVendido")

# PRODUTOS PELA SOMA DE ORDERQTY (DECRESC.)
df_sorted = df_soma_g.orderBy(col("TotalVendido").desc())

# 3 PRODUTOS MAIS VENDIDOS E MOSTRAR
df_top_3_produtos = df_sorted.limit(3)
df_top_3_produtos.show()



# TÓPICO 3 - 
df_juncao_3 = df_Sales_Customer.join(df_Person, df_Sales_Customer["PersonID"] == df_Person["BusinessEntityID"]) \
                             .join(df_Sales_Header, df_Sales_Customer["CustomerID"] == df_Sales_Header["CustomerID"])

# CAMPOS COM NOMES COMPLETOS E CONTAGEM POR NOME 
df_campo = df_juncao_3.select(df_Person["FirstName"].alias("Primeiro Nome"),
                             df_Person["LastName"].alias("Ultimo Nome"),
                             df_Sales_Customer["CustomerID"],
                             df_Sales_Header["SalesOrderID"])
df_cont = df_campo.groupBy("Primeiro Nome", "Ultimo Nome", "CustomerID") \
                           .agg({"SalesOrderID": "count"}) \
                           .withColumnRenamed("count(SalesOrderID)", "Pedidos Efetuados")
df_cont.show()


#TÓPICO 4
# JUNÇÃO DE TABELAS
df_juncao_4 = df_Sales_Header.join(df_Sales_Detail, "SalesOrderID") \
                          .join(df_Product, "ProductID")

# SELECIONAR COLUNAS + SOMA TOTAL
df_campo = df_juncao_4.select(df_Product["ProductID"], df_Sales_Header["OrderDate"], df_Sales_Detail["OrderQty"])
df_soma_total = df_campo.groupBy("ProductID", "OrderDate").agg({"OrderQty": "sum"}) \
                        .withColumnRenamed("sum(OrderQty)", "TotalProdutos")
df_soma_total.show()



# TÓPICO 5- FILTAR ORDER E ORDENAR (DECRESC.)
df_filtrado_5 = df_Sales_Header.filter((col("OrderDate") >= "2011-09-01") & (col("OrderDate") < "2011-10-01") & (col("TotalDue") > 1000))
df_resultado = df_filtrado_5.select("SalesOrderID", "OrderDate", "TotalDue")
df_resultado = df_resultado.orderBy(col("TotalDue").desc())

# RESULTADO
df_resultado.show()
