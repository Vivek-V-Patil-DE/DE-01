 #--Name : HTH
#--Owner : Sayu Softtech
#--Date cre : 08102022
#--Modified date : 
#--For more info follow us on instagram


from pyspark.sql import SparkSession
import datetime
spark = SparkSession.builder.appName("hbtohive_debit_details").master("yarn").enableHiveSupport().getOrCreate()

currentdate = datetime.datetime.now().strftime("%Y-%m-%d")

print("Credit card pending bill extraction job started")

bill_report=spark.sql("""
						SELECT
						CD.CardNumber as cardNumber,
						CD.CardType as CardType,
						CD.full_name as full_name,
						CD.contactnumber as mobile,
						CD.emailId as email,
						concat(CD.address,'_',CD.city,'_',CD.country) as cust_address,
						CD.BillingDate as billdate,
						CD.CardLimit as CardLimit,
						CD.cardLimit-D.totalDebitAmount+C.totalCreditAmount as outstandingLimit,
						D.totalDebitAmount-C.totalCreditAmount as outstandingAmount
						from prod.card_details CD
						LEFT JOIN
						(select cardNumber,sum(DebitAmount) as totalDebitAmount from prod.debit_details where UPPER(debitTxStatus)='SUCCESS' group by cardNumber)D on CD.cardNumber=D.CardNumber
						LEFT JOIN
						(select cardNumber,sum(CreditAmount) as totalCreditAmount from prod.credit_details where UPPER(CreditTxStatus)='SUCCESS' group by cardNumber) C ON CD.cardNumber=C.CardNumber
						where CD.Active_flag='A' and CD.cardLimit-D.totalDebitAmount+C.totalCreditAmount <> CD.cardLimit
""")

bill_report.show()

bill_report.coalesce(1).write.mode("append").format("CSV").option("header","true").option("delimiter","|").save("s3://b9class123/Oustanding_amt_report/bill_report_"+currentdate)

print("Credit Card pending bill extraction job finished sucessfully and data stored in s3")

spark.stop()