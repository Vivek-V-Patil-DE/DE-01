#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hbtohive_card_details").master("yarn").enableHiveSupport().getOrCreate()


print("Card details historical data computation started")

df_cd_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CARD_HB").option("zkUrl","localhost:2181").load()
df_ad_tmp=spark.read.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").load()
df_ct_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").load()
df_cn_tmp=spark.read.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").load()

df_cd_tmp.createOrReplaceTempView("Card_tmp")
df_ad_tmp.createOrReplaceTempView("Address_tmp")
df_ct_tmp.createOrReplaceTempView("City_tmp")
df_cn_tmp.createOrReplaceTempView("Country_tmp")

df_result=spark.sql("""SELECT
				C.c_number as CardNumber,
				C.c_type as CardType,
				C.full_name as CardHolderName,
				C.mob as contactnumber,
				C.email as emailId,
				Ad.street as address,
				ct.ct_name as city,
				cn.cn_name as country,
				C.issue_date as issuedate,
				C.update_date as update_date,
				C.billing_date as BillingDate,
				C.c_limit as CardLimit,
				C.act_flag as Active_flag
				from Card_tmp C
				LEFT JOIN Address_tmp Ad on C.add_id=Ad.add_id
				Left Join City_tmp ct on Ad.ct_id=ct.ct_id
				Left join Country_tmp cn on ct.cn_id=cn.cn_id
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.card_details_staging")


spark.catalog.dropTempView("Card_tmp")
spark.catalog.dropTempView("Address_tmp")
spark.catalog.dropTempView("City_tmp")
spark.catalog.dropTempView("Country_tmp")



#DSL
test=df_cd_tmp.join(df_ad_tmp,"add_id", how="left").join(df_ct_tmp,"ct_id", how="left").join(df_cn_tmp, "cn_id", how="left").drop("ADD_ID").drop("CT_ID").drop("CN_ID")

#Give alias

res=test.selectExpr("C_NUMBER as CardNumber", "C_TYPE as CardType","FULL_NAME as CardHolderName","MOB as contactnumber","EMAIL as emailId","STREET as address","CT_NAME as city","CN_NAME as country","ISSUE_DATE as issuedate","UPDATE_DATE as update_date","BILLING_DATE as BillingDate","C_LIMIT as CardLimit","ACT_FLAG as Active_flag")

res.show(5)

#Write Data in Hive
res.write.mode('overwrite').saveAsTable("prod.card_details_staging")

print("card details data written successfully in hive staging")


spark.stop()