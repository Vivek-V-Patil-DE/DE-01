#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hbtohive_credit_details").master("yarn").enableHiveSupport().getOrCreate()


print("credit details historical data computation started")

df_pd_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_PAID_HB").option("zkUrl","localhost:2181").load()
df_tt_tmp=spark.read.format("org.apache.phoenix.spark").option("table","TX_TYPE_HB").option("zkUrl","localhost:2181").load()

df_pd_tmp.createOrReplaceTempView("credit_tmp")
df_tt_tmp.createOrReplaceTempView("txtype_tmp")


df_result=spark.sql("""SELECT
				P.c_number as CardNumber,
				P.tx_id as TransactionId,
				P.tx_date as TransacrionTime,
				P.amt_paid as CreditAmount,
				P.c_status as CreditTxStatus,
				tt.tx_type_desc as paidBy
				from credit_tmp P 
				LEFT JOIN txtype_tmp tt on p.tx_type_id=tt.tx_type_id
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.credit_details_staging")

spark.catalog.dropTempView("credit_tmp")
spark.catalog.dropTempView("txtype_tmp")

#DSL
test=df_pd_tmp.join(df_tt_tmp,"tx_type_id", how="left").drop("TX_TYPE_ID")

#Give alias

res=test.selectExpr("C_NUMBER as CardNumber","TX_ID as TransactionId","TX_DATE as TransacrionTime","AMT_PAID as CreditAmount","C_STATUS as CreditTxStatus","TX_TYPE_DESC as paidBy")

res.show(5)

#Write Data in Hive
res.write.mode('overwrite').saveAsTable("prod.credit_details_staging")

print("credit details data written successfully in hive staging")

spark.stop()