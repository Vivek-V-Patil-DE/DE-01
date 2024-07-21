#--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc


spark = SparkSession.builder.appName("hbtohive_card_details").master("yarn").enableHiveSupport().getOrCreate()

#########################################Card Details########################

print("Card details de-duplication and compaction started")
cd_dff=spark.sql("select * from prod.card_details")
cd_dfs=spark.sql("select * from prod.card_details_staging")

un = cd_dff.union(cd_dfs)

res=un.withColumn("new_upd",F.when(un.update_date.isNull(),F.to_timestamp(F.lit("1970-01-01 00:00:00 "),format="yyyy-MM-dd HH:mm:SS")).otherwise(un.update_date)).drop("update_date")


f=res.select("*").withColumnRenamed("new_upd","update_date")

res1=f.withColumn("rn", F.row_number().over(Window.partitionBy("CardNumber").orderBy(desc("update_date"))))

res2=res1.filter(res1.rn == 1).drop("rn")

res2.show(30)

res2.write.mode('overwrite').saveAsTable("prod.card_details_temp")
spark.sql("truncate table prod.card_details")
spark.sql("insert overwrite table prod.card_details select cardnumber ,cardtype,full_name,contactnumber,emailid,address,city,country,issuedate,update_date,billingdate,cardlimit,active_flag from prod.card_details_temp")

spark.sql("truncate table prod.card_details_staging")
spark.sql("drop table prod.card_details_temp")

print("Card details de-duplication finished successfully and data pushed into final table")

#################################################Card Details Finished ###############

#########################################Debit Details########################

print("debit details de-duplication and compaction started")
d_dff=spark.sql("select * from prod.debit_details")
d_dfs=spark.sql("select * from prod.debit_details_staging")

un1 = d_dff.union(d_dfs)

res11=un1.dropDuplicates()

res11.show(30)

res11.write.mode('overwrite').saveAsTable("prod.debit_details_temp")
spark.sql("insert overwrite table prod.debit_details select cardnumber,transactionid,transacriontime,debitamount,spendcategory,debittxstatus from prod.debit_details_temp")
spark.sql("truncate table prod.debit_details_staging")
spark.sql("drop table prod.debit_details_temp")

print("debit details de-duplication finished successfully and data pushed into final table")

#################################################Card Details Finished ###############

#########################################Credit Details########################

print("credit details de-duplication and compaction started")
c_dff=spark.sql("select * from prod.credit_details")
c_dfs=spark.sql("select * from prod.credit_details_staging")

un2 = c_dff.union(c_dfs)

res22=un2.dropDuplicates()

res22.show(30)

res22.write.mode('overwrite').saveAsTable("prod.credit_details_temp")
spark.sql("insert overwrite table prod.credit_details select cardnumber,transactionid,transacriontime,creditamount,credittxstatus,paidby from prod.credit_details_temp")
spark.sql("truncate table prod.credit_details_staging")
spark.sql("drop table prod.credit_details_temp")

print("credit details de-duplication finished successfully and data pushed into final table")

#################################################Card Details Finished ###############


spark.stop()
