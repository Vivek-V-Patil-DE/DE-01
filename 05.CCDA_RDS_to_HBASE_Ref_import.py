#--Name : RTB
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#To more info visit us on instagram



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHBASE_Ref_Table").master("yarn").getOrCreate()

host="jdbc:postgresql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:5432/PROD"
user="puser"
pwd="ppassword"
driver="org.postgresql.Driver"

#dbtable="COUNTRY"

print("Data import for reference table is started....")
#Country Table
df_cn=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","COUNTRY").load()



df_cn.write.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl","localhost:2181").mode('overwrite').save()

print("Country Table Imported successfully")

#City Table
df_ct=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CITY").load()

df_ct.write.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("City Table Imported successfully")

#Tx_Type Table
df_ppr=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","Tx_Type").load()

df_ppr.write.format("org.apache.phoenix.spark").option("table","TX_TYPE_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("Tx_Type Table Imported successfully")

#CARD_TYPE
df_ppo=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CARD_TYPE").load()
df_ppo.write.format("org.apache.phoenix.spark").option("table","CARD_TYPE_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("CARD_TYPE Table Imported successfully")

print("All reference data imported sucessfully")

#df.createOrReplaceTempView("tab1")
#df2=spark.sql("select ct_id,ct_name from tab1")
#df.write.format("csv").save("/user/sagar/test1")
#df.show(5)

spark.stop()