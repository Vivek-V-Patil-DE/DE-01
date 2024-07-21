#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hbtohive_debit_details").master("yarn").enableHiveSupport().getOrCreate()


print("Debit details historical data computation started")

df_txtp_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl","localhost:2181").load()

df_txtp_tmp.createOrReplaceTempView("debit_tmp")


df_result=spark.sql("""SELECT
				D.c_number as CardNumber,
				D.tx_id as TransactionId,
				D.tx_date as TransacrionTime,
				D.amt_spend as DebitAmount,
				D.category as SpendCategory,
				D.d_status as debitTxStatus
				from debit_tmp D
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.debit_details_staging")

spark.catalog.dropTempView("debit_tmp")

#Give alias

res=df_txtp_tmp.selectExpr("C_NUMBER as CardNumber","TX_ID as TransactionId","TX_DATE as TransacrionTime","AMT_SPEND as DebitAmount","CATEGORY as SpendCategory","D_STATUS as debitTxStatus")

res.show(5)

#Write Data in Hive
res.write.mode('overwrite').saveAsTable("prod.debit_details_staging")

print("debit details data written successfully in hive staging")

spark.stop()