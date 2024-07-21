#--Name : RTB
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#For more info visit us on instagram

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDStoHDFS_Trans_Table").master("yarn").getOrCreate()
host="jdbc:postgresql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:5432/PROD"
user="puser"
pwd="ppassword"
driver="org.postgresql.Driver"


print("Historical Data import for transactional table is started....")

#CARD Table
df_sb=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CARD").load()



df_sb.write.format("org.apache.phoenix.spark").option("table","CARD_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("CARD Table Imported successfully")

#ADDRESS Table
df_ad=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","ADDRESS").load()
df_ad.write.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("ADDRESS Table Imported successfully")

#CC_Debit Table
df_sf=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CC_Debit").load()
df_sf.write.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("CC_Debit Table Imported successfully")

#CC_Paid Table
df_cm=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CC_Paid").load()
df_cm.write.format("org.apache.phoenix.spark").option("table","CC_PAID_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("CC_Paid Table Imported successfully")

print("All Historical data of transactional table imported successfully")

#df.createOrReplaceTempView("tab1")
#df2=spark.sql("select ct_id,ct_name from tab1")
#df.write.format("csv").save("/user/sagar/test1")
#df.show(5)

spark.stop()