#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHDFS_Trans_Table").master("local").enableHiveSupport().getOrCreate()

card_path="s3://b9class123/ccinputdata/ccda_data/card/*"
debit_path="s3://b9class123/ccinputdata/ccda_data/debit/*"
credit_path="s3://b9class123/ccinputdata/ccda_data/credit/*"
caddress_path="s3://b9class123/ccinputdata/ccda_data/caddress/*"


#Storing data into Hbase
df_card=spark.read.format("csv").option("header","true").option("inferschema","true").load(card_path)
#df_card.write.format("org.apache.phoenix.spark").option("table","CARD_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_cadd=spark.read.format("csv").option("header","true").option("inferschema","true").load(caddress_path)
#df_cadd.write.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_db=spark.read.format("csv").option("header","true").option("inferschema","true").load(debit_path)
#df_db.write.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl","localhost:2181").mode('overwrite').save()


df_cr=spark.read.format("csv").option("header","true").option("inferschema","true").load(credit_path)
#df_cr.write.format("org.apache.phoenix.spark").option("table","CC_PAID_HB").option("zkUrl","localhost:2181").mode('overwrite').save()

df_card.show()
df_cadd.show()
df_db.show()
df_cr.show()


print("Data Successfully Stored in Hbase")

df_tx=spark.read.format("org.apache.phoenix.spark").option("table","TX_TYPE_HB").option("zkUrl","localhost:2181").load()
df_ct_tmp1=spark.read.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").load()
df_cn_tmp1=spark.read.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").load()


df_cr.createOrReplaceTempView("credit_tmp1")
df_db.createOrReplaceTempView("debit_tmp1")
df_tx.createOrReplaceTempView("txtype_tmp1")

df_card.createOrReplaceTempView("Card_tmp1")
df_cadd.createOrReplaceTempView("Address_tmp1")
df_ct_tmp1.createOrReplaceTempView("City_tmp1")
df_cn_tmp1.createOrReplaceTempView("Country_tmp1")

#################################################
print("Processing Context for Card Details")

df_result_card=spark.sql("""SELECT
				C.C_NUMBER AS CARDNUMBER,
				C.C_TYPE AS CARDTYPE,
				C.FULL_NAME AS CARDHOLDERNAME,
				C.MOB AS CONTACTNUMBER,
				C.EMAIL AS EMAILID,
				AD.STREET AS ADDRESS,
				CT.CT_NAME AS CITY,
				CN.CN_NAME AS COUNTRY,
				C.ISSUE_DATE AS ISSUEDATE,
				C.UPDATE_DATE AS UPDATE_DATE,
				C.BILLING_DATE AS BILLINGDATE,
				C.C_LIMIT AS CARDLIMIT,
				C.ACT_FLAG AS ACTIVE_FLAG
				FROM CARD_TMP1 C
				LEFT JOIN ADDRESS_TMP1 AD ON C.ADD_ID=AD.ADD_ID
				LEFT JOIN CITY_TMP1 CT ON AD.CT_ID=CT.CT_ID
				LEFT JOIN COUNTRY_TMP1 CN ON CT.CN_ID=CN.CN_ID
""")

df_result_card.show(10)

df_result_card.write.mode('overwrite').saveAsTable("prod.card_details_staging")
("Delta Processed for Credit Details")
#############################################

#################################################
print("Processing Context for Debit Details")


df_result_db=spark.sql("""SELECT
				D.C_NUMBER AS CARDNUMBER,
				D.TX_ID AS TRANSACTIONID,
				D.TX_DATE AS TRANSACRIONTIME,
				D.AMT_SPEND AS DEBITAMOUNT,
				D.CATEGORY AS SPENDCATEGORY,
				D.D_STATUS AS DEBITTXSTATUS
				FROM DEBIT_TMP1 D
""")

df_result_db.show(10)

df_result_db.write.mode('overwrite').saveAsTable("prod.debit_details_staging")
("Delta Processed for Credit Details")
#############################################

#################################################
print("Processing Context for Credit Details")
df_result_cd=spark.sql("""SELECT
				P.C_NUMBER AS CARDNUMBER,
				P.TX_ID AS TRANSACTIONID,
				P.TX_DATE AS TRANSACRIONTIME,
				P.AMT_PAID AS CREDITAMOUNT,
				P.C_STATUS AS CREDITTXSTATUS,
				TT.TX_TYPE_DESC AS PAIDBY
				FROM CREDIT_TMP1 P 
				LEFT JOIN TXTYPE_TMP1 TT ON P.TX_TYPE_ID=TT.TX_TYPE_ID
""")

df_result_cd.show(10)

df_result_cd.write.mode('overwrite').saveAsTable("prod.credit_details_staging")
("Delta Processed for Credit Details")
#############################################

spark.stop()