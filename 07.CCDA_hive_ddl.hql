----Name : Hive table
----Owner : Sayu Softtech
----Date cre : 08102022
----Modified date : 
--For more info visit us on instagram

create database prod;

use prod;

create table card_details_staging (CardNumber integer,CardType string,full_name string,contactnumber integer,emailId string,address string,city string,country string,issuedate timestamp,update_date timestamp,BillingDate integer,CardLimit integer,Active_flag Char(1))
Row format delimited
fields terminated by ',';

create table card_details (CardNumber integer,CardType string,full_name string,contactnumber integer,emailId string,address string,city string,country string,issuedate timestamp,update_date timestamp,BillingDate integer,CardLimit integer,Active_flag Char(1))
Row format delimited
fields terminated by ',';

--------
create table debit_details_staging (CardNumber integer,TransactionId string,TransacrionTime timestamp,DebitAmount integer,SpendCategory string,debitTxStatus string)
Row Format delimited
fields terminated by ',';

create table debit_details (CardNumber integer,TransactionId string,TransacrionTime timestamp,DebitAmount integer,SpendCategory string,debitTxStatus string)
Row Format delimited
fields terminated by ',';

------------------
create table credit_details_staging (CardNumber integer,TransactionId string,TransacrionTime timestamp,CreditAmount integer,CreditTxStatus string,paidBy string)
Row Format delimited
fields terminated by ',';

create table credit_details (CardNumber integer,TransactionId string,TransacrionTime timestamp,CreditAmount integer,CreditTxStatus string,paidBy string)
Row Format delimited
fields terminated by ',';

--completed---





