# Databricks notebook source
# DBTITLE 1,Connection Set Up

#Param_SecretScope=dbutils.widgets.get("SecretScope")
Param_JdbcUrlKeyDW="jdbc:sqlserver://rxglobalsqlsrvprod.database.windows.net:1433;database=rxglobalsqldwprod;user=PROD_AZURESQLDW_MOCA_RW@rxglobalsqlsrvprod;password=MOC@rx@gl0bal7!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=120;"
#dbutils.widgets.get("JdbcUrlKeyDW")
#Param_OdbcUrlKeyDW="DRIVER={ODBC Driver 11 for SQL Server};SERVER=rxglobalsqlsrvprod.database.windows.net;DATABASE=rxglobalsqldwprod;UID=rxglobaladmin;PWD=rx@gl0bal"
#dbutils.widgets.get("OdbcUrlKeyDW")
Param_BlobStorage = "rxglobalsacprodmoca"
#dbutils.widgets.get("BlobStorage")
param_BlobAccessKey = "81jGcBgGa71u15NSo23ANmTzi2MEYjugL6WZb8WyeEfNFDTSQ9yYyrx7JkOMJDKpWm3qKfs5NLxqK40gbuU0lA=="
#dbutils.widgets.get("BlobAccessKey")

# Set spark config details
accountInfo = "fs.azure.account.key."+ Param_BlobStorage+".blob.core.windows.net"
spark.conf.set(accountInfo,param_BlobAccessKey)
               #dbutils.secrets.get(Param_SecretScope,param_BlobAccessKey))

# SQL DW connector variables
blobDir = "/tempDir/"
blobContainer = "moc-analytics"
blobDirPath = "wasbs://" + blobContainer + "@" + Param_BlobStorage+".blob.core.windows.net" + blobDir

# COMMAND ----------

# DBTITLE 1,User Defined Function
# import pyodbc;
# def ExecuteSQLDWQuery(Query):
#   conn = pyodbc.connect(Param_OdbcUrlKeyDW)
#   cursor = conn.cursor()
#   conn.autocommit = True
#   cursor.execute(Query)
#   conn.close()

      
      
def ExecuteSQLDWLoadMain(tablename,Flag):
  if Flag == 'Y':
    SelectQuery="select * from MOCA."+tablename
    print(SelectQuery)
    Main_DF=spark.sql(SelectQuery)
    Main_DF_Count=Main_DF.count()
    #print(tablename,"Delta Count: ",Main_Count)
    if Main_DF_Count>0:
      ExecDeleteQuery = "Truncate Table MOCA."+tablename+"';"
      #ExecuteSQLDWQuery(ExecDeleteQuery)
      print("Deleted SQL DW data for "+tablename)
      Main_DF.write.format("com.databricks.spark.sqldw")\
      .mode("overwrite")\
      .option("overwriteSchema", "true")\
      .option("url",Param_JdbcUrlKeyDW)\
      .option("tempDir",blobDirPath)\
      .option("maxStrLength", "2000")\
      .option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable","MOCA."+tablename)\
      .save()
      print("SQL DW Load Completed for "+tablename+" and count :"+str(Main_DF_Count))
      
import sys
from pyspark.sql.window import Window;
import pyspark.sql.functions as func;


# COMMAND ----------

# DBTITLE 1,Last 12 Months date
df=spark.sql("""
select distinct cast(concat(calen_year,lpad(calen_mo,2,'0')) as int) as MonthYear,calen_year,calen_mo 
from MOCA.D_CALENDAR where
cast(months_between (current_date(),CALEN_DT) as int )<13 and calen_dt<current_date()
""")

df.createOrReplaceTempView("DimMonthYear")

# COMMAND ----------

# DBTITLE 1,Country List
df=spark.sql("""select distinct country_id from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE
UNION
select distinct country_id from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY
UNION
select distinct country_id from MOCA.F_CER_PORTAL
UNION
select distinct country_id from MOCA.F_CER_EDETAIL_FINAL

""")
df.createOrReplaceTempView("CountryList")

# COMMAND ----------

# DBTITLE 1,CountryBrand List
df=spark.sql("""select distinct country_id,GLOBAL_PRODUCT_ID as GLOBAL_PRODUCT_ID from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE
UNION
select distinct country_id,GLOBAL_PRODUCT_ID as GLOBAL_PRODUCT_ID from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY
UNION
select distinct country_id,GLOBAL_PRODUCT_ID as GLOBAL_PRODUCT_ID from MOCA.F_CER_PORTAL
UNION
select distinct country_id,GLOBAL_PRODUCT_ID as GLOBAL_PRODUCT_ID from MOCA.F_CER_EDETAIL_FINAL

""")
df.createOrReplaceTempView("CountryBrandList")

# COMMAND ----------

# DBTITLE 1,CountryWiseMonthYear
df=spark.sql("""select country_id,MonthYear,calen_year,calen_mo,0 as count 
 from CountryList
cross join DimMonthYear""")
df.createOrReplaceTempView("CountryWiseMonthYear")

# COMMAND ----------

# DBTITLE 1,Country and Brand wise MonthYear
df=spark.sql("""select country_id,global_product_id,MonthYear,calen_year,calen_mo,0 as count 
from CountryBrandList
cross join DimMonthYear""")
df.createOrReplaceTempView("CountryBrandWiseMonthYear")

# COMMAND ----------

# DBTITLE 1,Brand Level Incremental HCP
#MASS EMail
dfBrandIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) CUSTOMER_ID,B.MonthYear,B.country_id,B.GLOBAL_PRODUCT_ID

from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE A 
inner join 
(select lower(customer_id) customer_id,country_id,GLOBAL_PRODUCT_ID, min(concat(calen_year,lpad(calen_mo,2,'0')))as MonthYear 
from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE 
where cast(EMAIL_CLICKED as int)>0
 group by  lower(customer_id),country_id,GLOBAL_PRODUCT_ID)B 
 on 
 lower(A.customer_id)=lower(B.customer_id )
 and A.GLOBAL_PRODUCT_ID=B.GLOBAL_PRODUCT_ID
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfBrandIncremental.createOrReplaceTempView("F_CER_MASS_BRAND_NEW_HCP")

# df=spark.sql("""select distinct country_id,global_product_id from F_CER_MASS_BRAND_NEW_HCP""")
# df.createOrReplaceTempView("MassBrands")

# df=spark.sql("""select country_id,global_product_id,MonthYear,calen_year,calen_mo,0 as count 
# from MassBrands
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Mass_Intermediate")

dfBrandIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id,Global_Product_ID) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'Mass' as channel
from(
select d.COUNTRY_ID,d.Global_Product_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryBrandWiseMonthYear d 
left outer JOIN F_CER_MASS_BRAND_NEW_HCP a 
on d.country_id=a.country_id and d.global_product_id=a.global_product_id and d.MonthYear=a.MonthYear
group by d.country_id,d.global_product_id,d.MonthYear) a 
""")
dfBrandIncr.createOrReplaceTempView("F_CER_MASS_BRAND_MONTHLY_NEW_HCP")

#OneToOneEmail
dfBrandIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) CUSTOMER_ID,B.MonthYear,B.country_id,B.GLOBAL_PRODUCT_ID 

from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY A 
inner join 
(select lower(customer_id) customer_id,country_id,GLOBAL_PRODUCT_ID, min(concat(calen_year,lpad(calen_mo,2,'0')))as MonthYear 
from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY 
where cast(CLICKED_EMAIL as int)>0
 group by  lower(customer_id),country_id,GLOBAL_PRODUCT_ID)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.GLOBAL_PRODUCT_ID=B.GLOBAL_PRODUCT_ID
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfBrandIncremental.createOrReplaceTempView("F_CER_ONETOONE_BRAND_NEW_HCP")

# df=spark.sql("""select distinct country_id,global_product_id from F_CER_ONETOONE_BRAND_NEW_HCP""")
# df.createOrReplaceTempView("MassBrands")

# df=spark.sql("""select country_id,global_product_id,MonthYear,calen_year,calen_mo,0 as count 
# from MassBrands
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Mass_Intermediate")

dfBrandIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id,Global_Product_ID) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'1:1' as channel
from(
select d.COUNTRY_ID,d.Global_Product_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryBrandWiseMonthYear d 
left outer JOIN F_CER_ONETOONE_BRAND_NEW_HCP a 
on d.country_id=a.country_id and d.global_product_id=a.global_product_id and d.MonthYear=a.MonthYear
group by d.country_id,d.global_product_id,d.MonthYear) a 
""")
dfBrandIncr.createOrReplaceTempView("F_CER_ONETOONE_BRAND_MONTHLY_NEW_HCP")

#Portal
dfBrandIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) CUSTOMER_ID,B.MonthYear,B.country_id,B.GLOBAL_PRODUCT_ID as GLOBAL_PRODUCT_ID

from MOCA.F_CER_PORTAL A 
inner join 
(select lower(customer_id) customer_id,country_id,GLOBAL_PRODUCT_ID, min(concat(calen_year,lpad(calen_mo,2,'0')))as MonthYear 
from MOCA.F_CER_PORTAL 
where cast(Portal_ENGAGE as int)>0
 group by  lower(customer_id),country_id,GLOBAL_PRODUCT_ID)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.GLOBAL_PRODUCT_ID=B.GLOBAL_PRODUCT_ID
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfBrandIncremental.createOrReplaceTempView("F_CER_PORTAL_BRAND_NEW_HCP")

# df=spark.sql("""select distinct country_id,global_product_id from F_CER_PORTAL_BRAND_NEW_HCP""")
# df.createOrReplaceTempView("MassBrands")

# df=spark.sql("""select country_id,global_product_id,MonthYear,calen_year,calen_mo,0 as count 
# from MassBrands
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Mass_Intermediate")

dfBrandIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id,Global_Product_ID) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'Portal' as channel
from(
select d.COUNTRY_ID,d.Global_Product_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryBrandWiseMonthYear d 
left outer JOIN F_CER_PORTAL_BRAND_NEW_HCP a 
on d.country_id=a.country_id and d.global_product_id=a.global_product_id and d.MonthYear=a.MonthYear
group by d.country_id,d.global_product_id,d.MonthYear) a 
""")
dfBrandIncr.createOrReplaceTempView("F_CER_PORTAL_BRAND_MONTHLY_NEW_HCP")

#Edetail
dfBrandIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) CUSTOMER_ID,B.MonthYear,B.country_id,lower(B.GLOBAL_PRODUCT_ID) as GLOBAL_PRODUCT_ID

from MOCA.F_CER_EDETAIL_FINAL A 
inner join 
(select lower(customer_id) customer_id,country_id,GLOBAL_PRODUCT_ID, min(concat(calen_year,lpad(calen_mo,2,'0')))as MonthYear 
from MOCA.F_CER_EDETAIL_FINAL 
where cast(EDETAIL_ENGAGE as int )>0
 group by  lower(customer_id),country_id,GLOBAL_PRODUCT_ID)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.GLOBAL_PRODUCT_ID=B.GLOBAL_PRODUCT_ID
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfBrandIncremental.createOrReplaceTempView("F_CER_EDETAIL_BRAND_NEW_HCP")

# df=spark.sql("""select distinct country_id,global_product_id from F_CER_EDETAIL_BRAND_NEW_HCP""")
# df.createOrReplaceTempView("MassBrands")

# df=spark.sql("""select country_id,global_product_id,MonthYear,calen_year,calen_mo,0 as count 
# from MassBrands
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Mass_Intermediate")

dfBrandIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id,Global_Product_ID) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'EDetail' as channel
from(
select d.COUNTRY_ID,d.Global_Product_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryBrandWiseMonthYear d 
left outer JOIN F_CER_EDETAIL_BRAND_NEW_HCP a 
on d.country_id=a.country_id and d.global_product_id=a.global_product_id and d.MonthYear=a.MonthYear
group by d.country_id,d.global_product_id,d.MonthYear) a 
""")
dfBrandIncr.createOrReplaceTempView("F_CER_EDETAIL_BRAND_MONTHLY_NEW_HCP")

df=spark.sql("""select * from F_CER_MASS_BRAND_MONTHLY_NEW_HCP
Union
select * from F_CER_ONETOONE_BRAND_MONTHLY_NEW_HCP
union
select * from F_CER_PORTAL_BRAND_MONTHLY_NEW_HCP
union
select * from F_CER_EDETAIL_BRAND_MONTHLY_NEW_HCP
""")
df.createOrReplaceTempView("F_CER_BRAND_INCREMENTAL_HCP")
df=spark.sql("""
select COUNTRY_ID
,MonthYear as CALEN_MO_YEAR
,GLOBAL_PRODUCT_ID
,customer_count as MONTHLY_NEW_CUSTOMER_COUNT
,CUMSUM as MONTHLY_NEW_CUSTOMER_CUMULATIVE_COUNT
,channel as MARKETING_CHANNEL_NAME
from F_CER_BRAND_INCREMENTAL_HCP
""")
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_BRAND_INCREMENTAL_HCP")

# COMMAND ----------

# DBTITLE 1,Incremental HCP 
#MASS Email
dfMassIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) as CUSTOMER_ID,B.MonthYear,B.country_id
from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE A 
inner join 
(select lower(customer_id) customer_id,country_id, min(cast(concat(calen_year,lpad(calen_mo,2,'0')) as int )) as MonthYear 
from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE
where cast(EMAIL_CLICKED as int)>0
 group by lower(customer_id),country_id)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfMassIncremental.createOrReplaceTempView("F_CER_MASS_NEW_HCP")

#df=spark.sql("""select distinct country_id from MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE""")
#df.createOrReplaceTempView("MassEmail")

# df=spark.sql("""select country_id,MonthYear,calen_year,calen_mo,0 as count 
# from CountryList
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Mass_Email_Intermediate")

dfMassIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'Mass' as channel
from(
select d.COUNTRY_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryWiseMonthYear d 
left outer JOIN F_CER_MASS_NEW_HCP a 
on d.country_id=a.country_id and d.MonthYear=a.MonthYear
group by d.country_id,d.MonthYear) a 
""")
dfMassIncr.createOrReplaceTempView("F_CER_MASS_MONTHLY_NEW_HCP")

#1:1 Email

dfMassIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) as CUSTOMER_ID,B.MonthYear,B.country_id
from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY A 
inner join 
(select lower(customer_id) customer_id,country_id, min(cast(concat(calen_year,lpad(calen_mo,2,'0')) as int )) as MonthYear 
from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY 
where cast(CLICKED_EMAIL as int)>0
 group by  lower(customer_id) ,country_id)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfMassIncremental.createOrReplaceTempView("F_CER_ONETOONE_NEW_HCP")

# df=spark.sql("""select distinct country_id from MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY""")
# df.createOrReplaceTempView("MassEmail")

# df=spark.sql("""select country_id,MonthYear,calen_year,calen_mo,0 as count 
# from CountryList
# cross join DimMonthYear""")
# df.createOrReplaceTempView("One_Email_Intermediate")

dfMassIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'1:1' as channel
from(
select d.COUNTRY_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryWiseMonthYear d 
left outer JOIN F_CER_ONETOONE_NEW_HCP a 
on d.country_id=a.country_id and d.MonthYear=a.MonthYear
group by d.country_id,d.MonthYear) a 
""")
dfMassIncr.createOrReplaceTempView("F_CER_ONETOONE_MONTHLY_NEW_HCP")

#Portal
dfMassIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) as CUSTOMER_ID,B.MonthYear,B.country_id
from MOCA.F_CER_PORTAL A 
inner join 
(select lower(customer_id) customer_id ,country_id, min(cast(concat(calen_year,lpad(calen_mo,2,'0')) as int )) as MonthYear 
from MOCA.F_CER_PORTAL 
where cast(Portal_ENGAGE as int)>0
 group by  lower(customer_id),country_id)B 
 on lower(A.customer_id)=lower(B.customer_id )
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfMassIncremental.createOrReplaceTempView("F_CER_PORTAL_NEW_HCP")

# df=spark.sql("""select distinct country_id from MOCA.F_CER_PORTAL""")
# df.createOrReplaceTempView("Portal")

# df=spark.sql("""select country_id,MonthYear,calen_year,calen_mo,0 as count 
# from Portal
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Portal_Intermediate")

dfMassIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'Portal' as channel
from(
select d.COUNTRY_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryWiseMonthYear d 
left outer JOIN F_CER_PORTAL_NEW_HCP a 
on d.country_id=a.country_id and d.MonthYear=a.MonthYear
group by d.country_id,d.MonthYear) a 
""")
dfMassIncr.createOrReplaceTempView("F_CER_PORTAL_MONTHLY_NEW_HCP")


#Edetail
dfMassIncremental=spark.sql("""
select distinct lower(B.CUSTOMER_ID) as CUSTOMER_ID,B.MonthYear,B.country_id
from MOCA.F_CER_EDETAIL_FINAL A 
inner join 
(select lower(customer_id) customer_id,country_id, min(cast(concat(calen_year,lpad(calen_mo,2,'0')) as int )) as MonthYear 
from MOCA.F_CER_EDETAIL_FINAL 
where cast(EDETAIL_ENGAGE as int )>0
 group by  lower(customer_id),country_id)B 
 on lower(A.customer_id)=lower(B.customer_id) 
 and A.Country_ID=B.Country_ID
where concat(A.calen_year,lpad(calen_mo,2,'0'))<=B.MonthYear
""")
dfMassIncremental.createOrReplaceTempView("F_CER_EDETAIL_NEW_HCP")

# df=spark.sql("""select distinct country_id from MOCA.F_CER_EDETAIL_FINAL""")
# df.createOrReplaceTempView("Edetail")

# df=spark.sql("""select country_id,MonthYear,calen_year,calen_mo,0 as count 
# from Edetail
# cross join DimMonthYear""")
# df.createOrReplaceTempView("Edetail_Intermediate")

dfMassIncr=spark.sql("""
select *
,sum(customer_count) over(partition by (country_id) order by MonthYear ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as CUMSUM 
,'Edetail' as channel
from(
select d.COUNTRY_ID, d.MonthYear, nvl(count(distinct customer_id),0) as customer_count
from CountryWiseMonthYear d 
left outer JOIN F_CER_EDETAIL_NEW_HCP a 
on d.country_id=a.country_id and d.MonthYear=a.MonthYear
group by d.country_id,d.MonthYear) a 
""")
dfMassIncr.createOrReplaceTempView("F_CER_EDETAIL_MONTHLY_NEW_HCP")


df=spark.sql("""select * from F_CER_MASS_MONTHLY_NEW_HCP
Union
select * from F_CER_ONETOONE_MONTHLY_NEW_HCP
union
select * from F_CER_PORTAL_MONTHLY_NEW_HCP
union
select * from F_CER_EDETAIL_MONTHLY_NEW_HCP
""")
df.createOrReplaceTempView("F_CER_INCREMENTAL_HCP")
df=spark.sql("""
select COUNTRY_ID
,MonthYear as CALEN_MO_YEAR
,customer_count as MONTHLY_NEW_CUSTOMER_COUNT
,CUMSUM as MONTHLY_NEW_CUSTOMER_CUMULATIVE_COUNT
,channel as MARKETING_CHANNEL_NAME
from F_CER_INCREMENTAL_HCP
""")
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_INCREMENTAL_HCP")


# COMMAND ----------

ExecuteSQLDWLoadMain("F_CER_INCREMENTAL_HCP","Y")
ExecuteSQLDWLoadMain("F_CER_BRAND_INCREMENTAL_HCP","Y")