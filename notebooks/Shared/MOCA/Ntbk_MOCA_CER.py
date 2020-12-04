# Databricks notebook source
!pip install pyodbc

# COMMAND ----------


dbutils.widgets.removeAll()
dbutils.widgets.text("SecretScope","")
dbutils.widgets.text("JdbcUrlKeyDW","")

dbutils.widgets.text("OdbcUrlKeyDW","")
dbutils.widgets.text("BlobStorage","")
dbutils.widgets.text("BlobAccessKey","")

# COMMAND ----------


Param_SecretScope=dbutils.widgets.get("SecretScope")
# jdbc:sqlserver://rxglobalsqlsrvprod.database.windows.net:1433;database=RxGlobalSQLDWProd;user=rxglobaladmin@rxglobalsqlsrvprod;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
#Param_JdbcUrlKeyDW="jdbc:sqlserver://rxglobalsqlsrvprod.database.windows.net:1433;database=RxGlobalSQLDWProd;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated"
Param_JdbcUrlKeyDW="jdbc:sqlserver://rxglobalsqlsrvprod.database.windows.net:1433;database=rxglobalsqldwprod;user=PROD_AZURESQLDW_MOCA_RW@rxglobalsqlsrvprod;password=MOC@rx@gl0bal7!;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=120;"
#dbutils.widgets.get("JdbcUrlKeyDW")
# Param_OdbcUrlKeyDW="DRIVER={ODBC Driver 11 for SQL Server};SERVER=rxglobalsqlsrvprod.database.windows.net;DATABASE=rxglobalsqldwprod;UID=rxglobaladmin;PWD=rx@gl0bal"
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

# DBTITLE 1,User Defined Functions
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

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS MOCA;

# COMMAND ----------

# DBTITLE 1,MOCA.d_calendar
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/d_calendar/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.d_calendar") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CL_GLOBAL_PRODUCT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/d_cl_global_product/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CL_GLOBAL_PRODUCT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_ACCOUNT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_account/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_ACCOUNT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_PRODUCT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_product/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_PRODUCT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_CALL
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_call/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_CALL") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_HCP_SEGMENT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_hcp_segment/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_HCP_SEGMENT") 

# COMMAND ----------

# DBTITLE 1,D_CRM_APPROVED_DOCUMENT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_approved_document/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_APPROVED_DOCUMENT") 

# COMMAND ----------

# DBTITLE 1,MOCA.H_AC_CL_MSG_BRAND
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Campaign/h_ac_cl_msg_brand/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.H_AC_CL_MSG_BRAND") 

# COMMAND ----------

# DBTITLE 1,MOCA.H_CL_DOC_GLOBAL_PRODUCT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/h_cl_doc_global_product/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.H_CL_DOC_GLOBAL_PRODUCT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_COUNTRY
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/d_country",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.D_COUNTRY") 

# COMMAND ----------

# DBTITLE 1,MOCA.H_CL_DOC_RELATIONSHIP
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/h_cl_doc_relationship/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.H_CL_DOC_RELATIONSHIP") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CL_DOCUMENT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/d_cl_document/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CL_DOCUMENT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CL_DOCUMENT_TYPE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/d_cl_document_type/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.D_CL_DOCUMENT_TYPE") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CL_CAMPAIGN
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/d_cl_campaign/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.D_CL_CAMPAIGN") 

# COMMAND ----------

# DBTITLE 1,MOCA.F_MESSAGE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Campaign/f_message/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.F_MESSAGE") 

# COMMAND ----------

# DBTITLE 1,F_PORTAL_HIT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Analytics/f_portal_hit/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.F_PORTAL_HIT") 

# COMMAND ----------

# DBTITLE 1,D_AA_PAGE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Analytics/d_aa_page/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.D_AA_PAGE") 

# COMMAND ----------

# DBTITLE 1,H_AA_CL_HIT_BRAND
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Analytics/h_aa_cl_hit_brand/",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.H_AA_CL_HIT_BRAND") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_AC_CAMP_SUBJECT
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Adobe Campaign/d_ac_camp_subject/",header=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_AC_CAMP_SUBJECT") 

# COMMAND ----------

# DBTITLE 1,MOCA.F_DOCUMENTS
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/f_documents",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("MOCA.F_DOCUMENTS")

# COMMAND ----------

# DBTITLE 1,F_DOCUMENTS -FETCH LATEST VERSION DATA
#F_DOCUMENTS= spark.sql("""SELECT DOCUMENT_NUMBER,DOCUMENT_VERSION,ROW_NUMBER() OVER(PARTITION BY DOCUMENT_NUMBER ORDER BY DOCUMENT_VERSION DESC) AS REC_RK   #FROM MOCA.F_DOCUMENTS """)
#F_DOCUMENTS.write.mode("overwrite").saveAsTable("MOCA.F_DOCUMENTS_STG")

#F_DOCUMENTS= spark.sql("""SELECT DOCUMENT_NUMBER,DOCUMENT_TYPE_ID,DOCUMENT_VERSION,ROW_NUMBER() OVER(PARTITION BY DOCUMENT_NUMBER ORDER BY DOCUMENT_VERSION DESC) AS REC_RK   FROM MOCA.F_DOCUMENTS """)
#F_DOCUMENTS.write.mode("overwrite").saveAsTable("MOCA.F_DOCUMENTS_STG")

F_DOCUMENTS_STG= spark.sql("""SELECT DOCUMENT_ID,DOCUMENT_NUMBER,DOCUMENT_TYPE_ID,CAMPAIGN_ID,DOCUMENT_VERSION,CONTENT_PURPOSE,KEY_CONTENT,KEY_CONTENT_EXCEPTION_INDICATOR,ROW_NUMBER() OVER(PARTITION BY DOCUMENT_NUMBER ORDER BY DOCUMENT_VERSION DESC) AS REC_RK   FROM MOCA.F_DOCUMENTS """)
F_DOCUMENTS_STG.write.mode("overwrite").option("overwriteSchema",'true').saveAsTable("MOCA.F_DOCUMENTS_STG")

# COMMAND ----------

# DBTITLE 1,MOCA.F_CALL_KEY_MESSAGE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/f_call_key_message",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","True").saveAsTable("MOCA.F_CALL_KEY_MESSAGE")

# COMMAND ----------

# DBTITLE 1,MOCA.F_EMAIL_ACTIVITY
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/f_email_activity",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","True").saveAsTable("MOCA.F_EMAIL_ACTIVITY")

# COMMAND ----------

# DBTITLE 1,MOCA.F_SENT_EMAIL
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/f_sent_email",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_SENT_EMAIL")

# COMMAND ----------

# DBTITLE 1,MOCA.F_MC_CYCLE_PLAN_DETAIL
df=spark.read.format("delta").load("/mnt/root/Curated/Global/Activity/F_MC_CYCLE_PLAN_DETAIL/",header=True)
df.write.format("delta").mode("overwrite").partitionBy('REGION','MARKETNAME').saveAsTable("MOCA.F_MC_CYCLE_PLAN_DETAIL") 

# COMMAND ----------

# DBTITLE 1,MOCA.CRMF_D_COUNTRY
df=spark.read.format("delta").load("/mnt/root/Curated/Global/Activity/D_COUNTRY/",header=True)
df.write.format("delta").mode("overwrite").partitionBy('REGION','MARKETNAME').saveAsTable("MOCA.CRMF_D_COUNTRY") 

# COMMAND ----------

# DBTITLE 1,MOCA.CRMF_D_ACCOUNT
df=spark.read.format("delta").load("/mnt/root/Curated/Global/Activity/D_ACCOUNT/",header=True)
df.write.format("delta").mode("overwrite").partitionBy('REGION','MARKETNAME').saveAsTable("MOCA.CRMF_D_ACCOUNT") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRMF_PRODUCT_CATALOG
df=spark.read.format("delta").load("/mnt/root/Curated/Global/Activity/D_PRODUCT_CATALOG/",header=True)
df.write.format("delta").mode("overwrite").partitionBy('REGION','MARKETNAME').saveAsTable("MOCA.CRMF_D_PRODUCT_CATALOG") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_RECORD_TYPE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_record_type/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_RECORD_TYPE") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_PRESENTATION
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_presentation/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_PRESENTATION") 

# COMMAND ----------

# DBTITLE 1,MOCA.D_CRM_PRESENTATION_SLIDE
df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva CRM/d_crm_presentation_slide/",header=True,inferSchema=True)
df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CRM_PRESENTATION_SLIDE") 

# COMMAND ----------

# DBTITLE 1,Cycle Plan extract 
df=spark.sql("""
SELECT C.ACCOUNT_ID,
       F.PRODUCT_ID,      
       H.COUNTRY_ID
	           
FROM MOCA.F_MC_CYCLE_PLAN_DETAIL A
INNER JOIN MOCA.CRMF_D_ACCOUNT D ON A.ACCOUNT_ID=D.ACCOUNT_ID
INNER JOIN MOCA.CRMF_D_PRODUCT_CATALOG B ON A.PRODUCT_CATALOG_ID = B.PRODUCT_CATALOG_ID 
INNER JOIN MOCA.D_CRM_ACCOUNT C ON D.SOURCE_ACCOUNT_AID = C.SOURCE_ACCOUNT_AID
INNER JOIN MOCA.D_CRM_PRODUCT F ON F.SOURCE_PRODUCT_AID = B.SOURCE_PRODUCT_CATALOG_AID 
INNER JOIN MOCA.CRMF_D_COUNTRY G ON G.COUNTRY_ID = A.COUNTRY_ID
INNER JOIN MOCA.D_COUNTRY H ON G.COUNTRY_CODE = H.COUNTRY_ALPHA2_CODE

GROUP BY
C.ACCOUNT_ID,
       F.PRODUCT_ID, 
       H.COUNTRY_ID 
       """)


df.write.format("delta").mode("overwrite").option ("overwriteSchema","true").saveAsTable("MOCA.F_MC_CYCLE_PLAN_DETAIL_temp")

# COMMAND ----------

# DBTITLE 1,MASS Email

dfMassEmail=spark.sql("""
select DISTINCT
FM.SOURCE_GIGYA_AID AS SOURCE_GIGYA_AID
,nvl(lower(DA1.SOURCE_ACCOUNT_AID),lower(FM.SOURCE_GIGYA_AID)) AS CUSTOMER_ID
,CASE WHEN DA1.ACCOUNT_ID IS NULL THEN -1 ELSE DA1.SOURCE_ACCOUNT_AID  END AS VEEVA_ID
,-1 AS VISITOR_ID
,CASE WHEN DA1.ACCOUNT_ID IS NULL THEN -1 ELSE DA1.ACCOUNT_ID  END AS ACCOUNT_ID
,DA1.ACCOUNT_SPECIALTY
,CASE WHEN HCP.HCP_SEGMENT IS NULL OR HCP.HCP_SEGMENT ='' THEN 'X' ELSE HCP.HCP_SEGMENT END AS SEGMENT
,FM.MESSAGE_ID
,FM.COUNTRY_ID
,DC.CALEN_MO
,DC.CALEN_YEAR
,upper(CS.CAMP_SUBJECT) as CAMP_SUBJECT
--,fds.CONTENT_PURPOSE
--,concat(DC.CALEN_MO,'_',CALEN_YEAR) AS DATE_KEY
--,LEFT(DC.PERD_ID,6)as MOYR
,CASE WHEN DCC.CAMPAIGN_NAME IS NULL OR DCC.CAMPAIGN_NAME ='' THEN 'NA' ELSE DCC.CAMPAIGN_NAME END AS CAMPAIGN_ID
,DCC.CAMPAIGN_FULL_NAME
,FM.DOCUMENT_ID
,FM.DOCUMENT_NUMBER
--,HB.CL_BRAND_ID
,CASE WHEN DGLB.GLOBAL_PRODUCT_ID IS NULL THEN -1 ELSE DGLB.GLOBAL_PRODUCT_ID END AS GLOBAL_PRODUCT_ID 
,FM.EMAIL_CLICKED
,FM.EMAIL_OPENED
,FM.EMAIL_DELIVERED

FROM MOCA.F_MESSAGE FM
INNER JOIN MOCA.D_CALENDAR DC ON DC.PERD_ID=FM.CONTACT_DATE_ID
INNER JOIN MOCA.H_AC_CL_MSG_BRAND HB ON FM.MESSAGE_ID=HB.MESSAGE_ID
INNER JOIN MOCA.D_CL_GLOBAL_PRODUCT DGLB ON DGLB.GLOBAL_PRODUCT_ID=CL_BRAND_ID
LEFT OUTER JOIN MOCA.D_AC_CAMP_SUBJECT CS ON FM.CAMP_SUBJECT_ID = CS.CAMP_SUBJECT_ID
LEFT OUTER JOIN MOCA.D_CRM_ACCOUNT DA1 ON  FM.SOURCE_GIGYA_AID=DA1.SOURCE_GIGYA_AID and FM.SOURCE_GIGYA_AID is not null
 LEFT OUTER JOIN 
          (SELECT SOURCE_ACCOUNT_AID,MIN(HCP_SEGMENT) AS HCP_SEGMENT 
          FROM MOCA.D_CRM_HCP_SEGMENT WHERE HCP_SEGMENT != '' AND HCP_segment IS NOT NULL GROUP BY SOURCE_ACCOUNT_AID ) AS HCP
          ON DA1.SOURCE_ACCOUNT_AID=HCP.SOURCE_ACCOUNT_AID 
LEFT OUTER JOIN 
(SELECT DOCUMENT_NUMBER,CAMPAIGN_ID FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
GROUP BY DOCUMENT_NUMBER, CAMPAIGN_ID ) FDS
ON FM.DOCUMENT_NUMBER = FDS.DOCUMENT_NUMBER 
LEFT OUTER JOIN MOCA.D_CL_CAMPAIGN DCC
ON FDS.CAMPAIGN_ID = DCC.CAMPAIGN_ID           
WHERE FM.CAMPAIGN_DELIVERY_LABEL NOT LIKE '%test%'
 AND FM.CAMPAIGN_DELIVERY_LABEL NOT LIKE '%seed%'
 AND FM.CAMPAIGN_DELIVERY_LABEL NOT LIKE '%proof%'
 AND FM.CONTACT_DATE_ID > 0

AND   cast(months_between (current_date(),DC.CALEN_DT) as int )<=12
""")

dfMassEmail.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE")
#dfMassEmail.createOrReplaceTempView("F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE")

# COMMAND ----------

# DBTITLE 1,1:1 Email
dfOneToOneEmail=spark.sql("""
select distinct
country_id
,DOCUMENT_ID
,ACCOUNT_ID
,ACCOUNT_SPECIALTY
,GLOBAL_PRODUCT_ID
,DOCUMENT_NUMBER
,lower(SOURCE_GIGYA_AID) as SOURCE_GIGYA_AID
,lower(CUSTOMER_ID) as CUSTOMER_ID
,VEEVA_ID
,-1 AS VISITOR_ID
,SEGMENT
,CAMPAIGN_ID
,CAMPAIGN_FULL_NAME
,calen_mo
,CALEN_YEAR
,upper(EMAIL_SUBJECT) as EMAIL_SUBJECT
,max(TARGET_FLAG) as TARGET_FLAG
,(case WHEN COALESCE(Opened_vod,'0')='Opened_vod' THEN 1 ELSE 0 END )AS OPENED_EMAIL
,(case WHEN COALESCE(Clicked_vod,'0')='Clicked_vod' THEN 1 ELSE 0 END )AS CLICKED_EMAIL
,(case WHEN COALESCE(Delivered_vod,'0')='Delivered_vod' THEN 1 ELSE 0 END )AS DELIVERED_EMAIL
 from 
 (
SELECT 
DISTINCT
EA.country_id
,nvl(lower(ACC.source_gigya_aid),lower(ACC.SOURCE_ACCOUNT_AID)) as CUSTOMER_ID
,lower(ACC.source_gigya_aid) as source_gigya_aid
,ACC.SOURCE_ACCOUNT_AID AS VEEVA_ID
,CASE WHEN HCP.HCP_SEGMENT IS NULL OR HCP.HCP_SEGMENT ='' THEN 'X' ELSE HCP.HCP_SEGMENT END AS SEGMENT
,CASE WHEN AD.DOCUMENT_ID IS NULL THEN -1 ELSE AD.DOCUMENT_ID  END AS DOCUMENT_ID
,CASE WHEN EA.ACCOUNT_ID IS NULL THEN -1 ELSE EA.ACCOUNT_ID  END AS ACCOUNT_ID
,ACC.ACCOUNT_SPECIALTY
,DC.CALEN_MO
,DC.CALEN_YEAR
,upper(ad.EMAIL_SUBJECT) as EMAIL_SUBJECT
,EA.EMAIL_ACTIVITY_EVENT_TYPE
,CASE WHEN CL_PR.GLOBAL_PRODUCT_ID IS NULL THEN -1 ELSE CL_PR.GLOBAL_PRODUCT_ID  END AS GLOBAL_PRODUCT_ID
,CASE WHEN DCC.CAMPAIGN_NAME IS NULL OR DCC.CAMPAIGN_NAME ='' THEN 'NA' ELSE DCC.CAMPAIGN_NAME END AS CAMPAIGN_ID
,DCC.CAMPAIGN_FULL_NAME
,CL_DOC.DOCUMENT_NUMBER
,case when ( nvl(cy.ACCOUNT_ID,-1)!=-1 and nvl(cy.PRODUCT_ID,-1)!=-1 ) THEN '1' else '0' end as TARGET_FLAG

FROM MOCA.F_EMAIL_ACTIVITY EA
INNER JOIN MOCA.F_SENT_EMAIL SE
ON EA.SENT_EMAIL_ID = SE.SENT_EMAIL_ID
INNER JOIN MOCA.D_CALENDAR DC on SE.EMAIL_SENT_DATE_ID=DC.PERD_ID
LEFT OUTER JOIN moca.D_CRM_ACCOUNT ACC on EA.ACCOUNT_ID= ACC.ACCOUNT_ID
INNER JOIN MOCA.D_CRM_PRODUCT PR ON EA.PRODUCT_ID=PR.PRODUCT_ID
INNER JOIN MOCA.D_CRM_APPROVED_DOCUMENT AD on EA.APPROVED_DOCUMENT_ID= AD.APPROVED_DOCUMENT_ID
 LEFT OUTER JOIN 
          (SELECT SOURCE_ACCOUNT_AID,SOURCE_PRODUCT_AID,MIN(HCP_SEGMENT) AS HCP_SEGMENT 
          FROM MOCA.D_CRM_HCP_SEGMENT WHERE HCP_SEGMENT != '' AND HCP_segment IS NOT NULL GROUP BY SOURCE_ACCOUNT_AID,SOURCE_PRODUCT_AID ) AS HCP 
           ON ACC.SOURCE_ACCOUNT_AID=HCP.SOURCE_ACCOUNT_AID AND PR.SOURCE_PRODUCT_AID=HCP.SOURCE_PRODUCT_AID
LEFT OUTER JOIN
(SELECT HCL.DOCUMENT_ID,
	   D2.DOCUMENT_NUMBER
	FROM	MOCA.H_CL_DOC_RELATIONSHIP AS HCL
	join MOCA.D_CL_DOCUMENT D1 ON HCL.DOCUMENT_ID = D1.DOCUMENT_ID
	join MOCA.D_CL_DOCUMENT D2  ON HCL.PARENT_DOCUMENT_ID = D2.DOCUMENT_ID 
    WHERE HCL.DOCUMENT_RELATIONSHIP_TYPE = 'basedon__v' 
    AND LEFT(D1.DOCUMENT_NUMBER, 3) NOT IN ('MCP','MCS')) AS CL_DOC ON AD.DOCUMENT_ID=CL_DOC.DOCUMENT_ID

LEFT OUTER JOIN 
(SELECT DOCUMENT_NUMBER,F_DOC.DOCUMENT_VERSION,HGP.GLOBAL_PRODUCT_ID
 FROM MOCA.F_DOCUMENTS_STG AS F_DOC
 INNER JOIN MOCA.H_CL_DOC_GLOBAL_PRODUCT AS HGP ON F_DOC.DOCUMENT_VERSION=HGP.DOCUMENT_VERSION
 WHERE F_DOC.REC_RK=1
 GROUP BY DOCUMENT_NUMBER,F_DOC.DOCUMENT_VERSION,HGP.GLOBAL_PRODUCT_ID) AS CL_PR
 ON CL_DOC.DOCUMENT_NUMBER=CL_PR.DOCUMENT_NUMBER
 
 LEFT OUTER JOIN 
(SELECT DOCUMENT_NUMBER,CAMPAIGN_ID FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
GROUP BY DOCUMENT_NUMBER, CAMPAIGN_ID) FDS
ON CL_DOC.DOCUMENT_NUMBER = FDS.DOCUMENT_NUMBER 
LEFT OUTER JOIN MOCA.D_CL_CAMPAIGN DCC
ON FDS.CAMPAIGN_ID = DCC.CAMPAIGN_ID 

 LEFT OUTER JOIN MOCA.F_MC_CYCLE_PLAN_DETAIL_temp CY 
 on (EA.Account_ID=CY.Account_ID 
 and EA.PRODUCT_ID=CY.PRODUCT_ID 
 and EA.COUNTRY_ID=CY.COUNTRY_ID )

WHERE (EA.EMAIL_ACTIVITY_EVENT_TYPE = 'Opened_vod' OR EA.EMAIL_ACTIVITY_EVENT_TYPE = 'Clicked_vod' or EA.EMAIL_ACTIVITY_EVENT_TYPE = 'Delivered_vod')
  AND CL_DOC.DOCUMENT_NUMBER IS NOT NULL AND EA.SENT_EMAIL_ID <>-1

AND   cast(months_between (current_date(),DC.CALEN_DT) as int )<=12
GROUP BY
EA.country_id
--,EA.PRODUCT_ID
,EA.ACCOUNT_ID
,ACC.ACCOUNT_SPECIALTY
,nvl(lower(ACC.source_gigya_aid),lower(ACC.SOURCE_ACCOUNT_AID))
,ACC.SOURCE_GIGYA_AID
,ACC.SOURCE_ACCOUNT_AID
,HCP.HCP_SEGMENT
,AD.DOCUMENT_ID
,DC.CALEN_MO
,DC.CALEN_YEAR
,concat(DC.CALEN_MO,'_',DC.CALEN_YEAR)
,ad.EMAIL_SUBJECT
,EA.EMAIL_ACTIVITY_EVENT_TYPE
,CL_PR.GLOBAL_PRODUCT_ID
,DCC.CAMPAIGN_NAME
,CL_DOC.DOCUMENT_NUMBER
,DCC.CAMPAIGN_FULL_NAME
,cy.ACCOUNT_ID
,cy.PRODUCT_ID
)A
pivot 
(max(A.EMAIL_ACTIVITY_EVENT_TYPE) for EMAIL_ACTIVITY_EVENT_TYPE in ("Opened_vod","Clicked_vod","Delivered_vod"))
group by 
country_id
,DOCUMENT_ID
,ACCOUNT_ID
,ACCOUNT_SPECIALTY
,GLOBAL_PRODUCT_ID
,DOCUMENT_NUMBER
,SOURCE_GIGYA_AID 
,CUSTOMER_ID
,VEEVA_ID
,VISITOR_ID
,SEGMENT
,CAMPAIGN_ID
,CAMPAIGN_FULL_NAME
,calen_mo
,CALEN_YEAR
,EMAIL_SUBJECT
,OPENED_EMAIL
,CLICKED_EMAIL
,DELIVERED_EMAIL """)
#dfOneToOneEmail.createOrReplaceTempView("F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY")
dfOneToOneEmail.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY")

# COMMAND ----------

# DBTITLE 1,Portal
dfPortal_stage1=spark.sql("""
SELECT 
            nvl(nvl(lower(DCA.SOURCE_ACCOUNT_AID),lower(PH.SOURCE_GIGYA_AID)),lower(PH.POST_VISID_HIGH) || '_' || lower(PH.POST_VISID_LOW)) as CUSTOMER_ID
            ,lower(PH.SOURCE_GIGYA_AID) as SOURCE_GIGYA_AID
            ,(lower(PH.POST_VISID_HIGH) || '_' || lower(PH.POST_VISID_LOW)) AS VISITOR_ID
            ,CASE WHEN DCA.ACCOUNT_ID IS NULL THEN -1 ELSE DCA.SOURCE_ACCOUNT_AID  END AS VEEVA_ID
            ,CASE WHEN DCA.ACCOUNT_ID IS NULL THEN -1 ELSE DCA.ACCOUNT_ID  END AS ACCOUNT_ID
			,DCA.ACCOUNT_SPECIALTY
            ,CASE WHEN HCP.HCP_SEGMENT IS NULL OR HCP.HCP_SEGMENT ='' THEN 'X' ELSE HCP.HCP_SEGMENT END AS SEGMENT
			,PH.COUNTRY_ID
            ,DC.CALEN_MO
            ,DC.CALEN_YEAR
            --,concat(DC.CALEN_MO,'_',DC.CALEN_YEAR) AS DATE_KEY
            ,CASE WHEN DCC.CAMPAIGN_NAME IS NULL OR DCC.CAMPAIGN_NAME ='' THEN 'NA' ELSE DCC.CAMPAIGN_NAME END AS CAMPAIGN_ID
            ,DCC.CAMPAIGN_FULL_NAME
			,PH.DOCUMENT_ID
			,PH.DOCUMENT_NUMBER
            ,CASE WHEN HCB.CL_BRAND_ID IS NULL THEN -1 ELSE HCB.CL_BRAND_ID END AS GLOBAL_PRODUCT_ID
            ,(CASE
                WHEN PH.POST_EVENT_LIST LIKE '%204%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%220%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%222%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%253%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%255%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%258%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%283%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%284%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%299%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%20122%' THEN 1
                WHEN PH.POST_EVENT_LIST LIKE '%209%' THEN 1
              ELSE 0
              END
              ) AS CONVERTED
            ,(CASE
                WHEN PH.EXCLUDE_HIT > 0 THEN 0
                WHEN PH.HIT_SOURCE IN (5,7,8,9) THEN 0
              ELSE 1
               END) AS INCLUDE_HIT,
              
              (POST_VISID_HIGH || '_' || POST_VISID_LOW || '_' || VISIT_NUM) AS VISIT_ID
			
		  FROM MOCA.F_PORTAL_HIT PH 
          INNER JOIN MOCA.D_CALENDAR DC ON DC.PERD_ID=PH.VISIT_DATE_ID
          LEFT OUTER JOIN MOCA.H_AA_CL_HIT_BRAND HCB ON HCB.HIT_ID=PH.HIT_ID
          LEFT OUTER JOIN MOCA.D_CRM_ACCOUNT DCA ON PH.SOURCE_GIGYA_AID=DCA.SOURCE_GIGYA_AID --AND DCA.SOURCE_GIGYA_AID IS NOT NULL
          LEFT OUTER JOIN 
          (SELECT SOURCE_ACCOUNT_AID,MIN(HCP_SEGMENT) AS HCP_SEGMENT FROM MOCA.D_CRM_HCP_SEGMENT WHERE HCP_SEGMENT != '' AND HCP_segment IS NOT NULL GROUP BY SOURCE_ACCOUNT_AID ) AS HCP
          ON DCA.SOURCE_ACCOUNT_AID=HCP.SOURCE_ACCOUNT_AID  
          LEFT OUTER JOIN 
          (SELECT DOCUMENT_NUMBER,CAMPAIGN_ID FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
          GROUP BY DOCUMENT_NUMBER, CAMPAIGN_ID) FDS
          ON PH.DOCUMENT_NUMBER = FDS.DOCUMENT_NUMBER 
          LEFT OUTER JOIN MOCA.D_CL_CAMPAIGN DCC
           ON FDS.CAMPAIGN_ID = DCC.CAMPAIGN_ID 
          LEFT OUTER JOIN          
          (SELECT (VISIT_START_TIME_GMT || '_' || VISIT_NUM || '_' || POST_VISID_HIGH || '_' || POST_VISID_LOW ) AS VISIT_ID,
          HIT_ID FROM MOCA.F_PORTAL_HIT PH 
          LEFT JOIN MOCA.D_AA_PAGE P ON PH.PAGE_ID = P.PAGE_ID
          WHERE 
            PH.PAGE_URL_WITH_PARAM NOT LIKE '%gskpro%' AND (PH.PAGE_ID=-1 OR P.PAGE_NAME NOT LIKE '%gskpro%')
			AND PH.PAGE_URL_WITH_PARAM NOT LIKE '%gsksource%' AND (PH.PAGE_ID=-1 OR P.PAGE_NAME NOT LIKE '%gsksource%')
			AND PH.PAGE_URL_WITH_PARAM NOT LIKE '%gsksamples%' AND (PH.PAGE_ID=-1 OR P.PAGE_NAME NOT LIKE '%gsksamples%')
			AND PH.PAGE_URL_WITH_PARAM NOT LIKE '%healthgsk%' AND (PH.PAGE_ID=-1 OR P.PAGE_NAME NOT LIKE '%healthgsk%')
			AND PH.PAGE_URL_WITH_PARAM NOT LIKE '%health.gsk%' AND (PH.PAGE_ID=-1 OR P.PAGE_NAME NOT LIKE '%health.gsk%') ) AS GSK_PRO
            ON (PH.VISIT_START_TIME_GMT || '_' || PH.VISIT_NUM || '_' || PH.POST_VISID_HIGH || '_' || PH.POST_VISID_LOW ) = GSK_PRO.VISIT_ID  AND PH.HIT_ID = GSK_PRO.HIT_ID
            LEFT OUTER JOIN
            (SELECT (VISIT_START_TIME_GMT || '_' || VISIT_NUM || '_' || POST_VISID_HIGH || '_' || POST_VISID_LOW ) AS VISIT_ID
             FROM MOCA.F_PORTAL_HIT PH 
          LEFT JOIN MOCA.D_AA_PAGE P ON PH.PAGE_ID = P.PAGE_ID
	      WHERE P.PAGE_NAME LIKE '%accessgsk-page%') AS ACCESS_GSK
          ON (PH.VISIT_START_TIME_GMT || '_' || PH.VISIT_NUM || '_' || PH.POST_VISID_HIGH || '_' || PH.POST_VISID_LOW ) = ACCESS_GSK.VISIT_ID
           WHERE cast(months_between (current_date(),dc.calen_DT) as int ) <=12  
          AND PH.POST_EVENT_LIST LIKE '%,%' --TO EXCLUDE JUNKY RECORDS
          AND GSK_PRO.VISIT_ID IS NULL --TO EXCLUDE GSK INTERNAL and other not analysed visits
          AND ACCESS_GSK.VISIT_ID IS NULL 
         
          """)
dfPortal_stage1.createOrReplaceTempView("F_CER_PORTAL_STAGE1")

dfPortal_final=spark.sql("""SELECT PH.CUSTOMER_ID
,PH.SOURCE_GIGYA_AID
,PH.VISITOR_ID
            ,PH.VEEVA_ID
            ,PH.ACCOUNT_ID
			,PH.ACCOUNT_SPECIALTY
            ,PH.SEGMENT
			,PH.COUNTRY_ID
            ,PH.CALEN_MO
			,PH.CALEN_YEAR
			--,PH.DATE_KEY
            ,PH.CAMPAIGN_ID
            ,PH.CAMPAIGN_FULL_NAME
			,PH.DOCUMENT_ID
			,PH.DOCUMENT_NUMBER
            ,PH.GLOBAL_PRODUCT_ID
            ,PH.PORTAL_ENGAGE
            ,PH.PORTAL_REACH
            ,SUM(INCLUDE_HIT) AS PORTAL_VISITS
            FROM (
select  PH.CUSTOMER_ID,PH.SOURCE_GIGYA_AID,PH.VISITOR_ID
            ,PH.VEEVA_ID
            ,PH.ACCOUNT_ID
			,PH.ACCOUNT_SPECIALTY
            ,PH.SEGMENT
			,PH.COUNTRY_ID
            ,PH.CALEN_MO
			,PH.CALEN_YEAR
			--,PH.DATE_KEY
            ,PH.CAMPAIGN_ID
            ,PH.CAMPAIGN_FULL_NAME
			,PH.DOCUMENT_ID
			,PH.DOCUMENT_NUMBER
            ,PH.GLOBAL_PRODUCT_ID
            ,PH.CONVERTED AS PORTAL_ENGAGE
            ,PH.INCLUDE_HIT AS PORTAL_REACH
            ,CASE WHEN SUM(INCLUDE_HIT) >=1 THEN 1 ELSE 0 END AS INCLUDE_HIT 
        from F_CER_PORTAL_STAGE1 AS PH
            GROUP BY PH.CUSTOMER_ID,PH.SOURCE_GIGYA_AID,PH.VISITOR_ID
            ,PH.VEEVA_ID
            ,PH.ACCOUNT_ID
			,PH.ACCOUNT_SPECIALTY
            ,PH.SEGMENT
			,PH.COUNTRY_ID
            ,PH.CALEN_MO
			,PH.CALEN_YEAR
			--,PH.DATE_KEY
            ,PH.CAMPAIGN_ID
            ,PH.CAMPAIGN_FULL_NAME
			,PH.DOCUMENT_ID
			,PH.DOCUMENT_NUMBER
            ,PH.GLOBAL_PRODUCT_ID
            ,PH.VISIT_ID
            ,PH.CONVERTED
            ,PH.INCLUDE_HIT) AS PH
            GROUP BY 
            PH.CUSTOMER_ID,PH.SOURCE_GIGYA_AID,PH.VISITOR_ID
            ,PH.VEEVA_ID
            ,PH.ACCOUNT_ID
			,PH.ACCOUNT_SPECIALTY
            ,PH.SEGMENT
			,PH.COUNTRY_ID
            ,PH.CALEN_MO
			,PH.CALEN_YEAR
			--,PH.DATE_KEY
            ,PH.CAMPAIGN_ID
            ,PH.CAMPAIGN_FULL_NAME
			,PH.DOCUMENT_ID
			,PH.DOCUMENT_NUMBER
            ,PH.GLOBAL_PRODUCT_ID
            ,PH.PORTAL_REACH
            ,PH.PORTAL_ENGAGE
            """)


#dfPortal_final.createOrReplaceTempView("F_CER_PORTAL")
dfPortal_final.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_PORTAL")


# COMMAND ----------

# DBTITLE 1,F_CER_Edetail
dfEdetail=spark.sql("""
SELECT DISTINCT 
nvl(lower(ACC.SOURCE_ACCOUNT_AID),lower(ACC.SOURCE_GIGYA_AID)) as CUSTOMER_ID
,lower(ACC.SOURCE_GIGYA_AID) AS SOURCE_GIGYA_AID
,lower(ACC.SOURCE_ACCOUNT_AID) AS VEEVA_ID
,-1 AS VISITOR_ID
,CASE WHEN ED.ACCOUNT_ID IS NULL THEN -1 ELSE ED.ACCOUNT_ID  END AS ACCOUNT_ID
,ACC.ACCOUNT_SPECIALTY
,CASE WHEN HCP.HCP_SEGMENT IS NULL OR HCP.HCP_SEGMENT ='' THEN 'X' ELSE HCP.HCP_SEGMENT END AS SEGMENT
,ED.COUNTRY_ID
,DC.CALEN_MO
,DC.CALEN_YEAR
--,CONCAT(DC.CALEN_MO,'_',DC.CALEN_YEAR) AS DATE_KEY
,CASE WHEN DCC.CAMPAIGN_NAME IS NULL OR DCC.CAMPAIGN_NAME ='' THEN 'NA' ELSE DCC.CAMPAIGN_NAME END AS CAMPAIGN_ID
,dcc.CAMPAIGN_FULL_NAME
,CASE WHEN PRS.DOCUMENT_ID IS NULL THEN -1 ELSE PRS.DOCUMENT_ID  END AS DOCUMENT_ID
,CL_DOC.DOCUMENT_NUMBER
,CASE WHEN CL_PR.GLOBAL_PRODUCT_ID IS NULL THEN -1 ELSE CL_PR.GLOBAL_PRODUCT_ID  END AS GLOBAL_PRODUCT_ID
,ED.CALL_ID
,ED.KEY_MESSAGE_ID
,ED.DURATION
,case when ( nvl(cy.ACCOUNT_ID,-1)!=-1 and nvl(cy.PRODUCT_ID,-1)!=-1 ) THEN '1' else '0' end as TARGET_FLAG
FROM MOCA.F_CALL_KEY_MESSAGE ED
INNER JOIN MOCA.D_CRM_CALL C ON ED.CALL_ID=C.CALL_ID
INNER JOIN MOCA.D_CRM_RECORD_TYPE RT ON C.RECORD_TYPE_ID=RT.RECORD_TYPE_ID
INNER JOIN MOCA.D_CALENDAR DC ON DC.PERD_ID=C.CALL_DATE_ID
INNER JOIN MOCA.D_CRM_ACCOUNT ACC ON ED.ACCOUNT_ID=ACC.ACCOUNT_ID
INNER JOIN MOCA.D_CRM_PRODUCT PR ON ED.PRODUCT_ID=PR.PRODUCT_ID
INNER JOIN MOCA.D_CRM_PRESENTATION PRS ON ED.PRESENTATION_ID=PRS.PRESENTATION_ID
 LEFT OUTER JOIN 
          (SELECT SOURCE_ACCOUNT_AID,SOURCE_PRODUCT_AID,MIN(HCP_SEGMENT) AS HCP_SEGMENT FROM MOCA.D_CRM_HCP_SEGMENT WHERE HCP_SEGMENT != '' AND HCP_segment IS NOT NULL GROUP BY SOURCE_ACCOUNT_AID,SOURCE_PRODUCT_AID ) AS HCP 
           ON ACC.SOURCE_ACCOUNT_AID=HCP.SOURCE_ACCOUNT_AID AND PR.SOURCE_PRODUCT_AID=HCP.SOURCE_PRODUCT_AID
LEFT OUTER JOIN
(SELECT HCL.DOCUMENT_ID,
	   D2.DOCUMENT_NUMBER
	FROM	MOCA.H_CL_DOC_RELATIONSHIP AS HCL
	join MOCA.D_CL_DOCUMENT D1 ON HCL.DOCUMENT_ID = D1.DOCUMENT_ID
	join MOCA.D_CL_DOCUMENT D2 ON HCL.PARENT_DOCUMENT_ID = D2.DOCUMENT_ID WHERE HCL.DOCUMENT_RELATIONSHIP_TYPE = 'related_original_doc__v' AND LEFT(D1.DOCUMENT_NUMBER, 3) = 'MCP') AS CL_DOC
ON PRS.DOCUMENT_ID=CL_DOC.DOCUMENT_ID
LEFT OUTER JOIN 
  (SELECT DOCUMENT_NUMBER,F_DOC.DOCUMENT_VERSION,HGP.GLOBAL_PRODUCT_ID
   FROM MOCA.F_DOCUMENTS_STG AS F_DOC
   INNER JOIN MOCA.H_CL_DOC_GLOBAL_PRODUCT AS HGP ON F_DOC.DOCUMENT_VERSION=HGP.DOCUMENT_VERSION
   WHERE F_DOC.REC_RK=1
   GROUP BY DOCUMENT_NUMBER,F_DOC.DOCUMENT_VERSION,HGP.GLOBAL_PRODUCT_ID
   ) AS CL_PR
 ON CL_DOC.DOCUMENT_NUMBER=CL_PR.DOCUMENT_NUMBER
 LEFT OUTER JOIN 
(SELECT DOCUMENT_NUMBER,CAMPAIGN_ID FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
GROUP BY DOCUMENT_NUMBER, CAMPAIGN_ID) FDS
ON CL_DOC.DOCUMENT_NUMBER = FDS.DOCUMENT_NUMBER 

LEFT OUTER JOIN MOCA.D_CL_CAMPAIGN DCC
ON FDS.CAMPAIGN_ID = DCC.CAMPAIGN_ID 

LEFT OUTER JOIN MOCA.F_MC_CYCLE_PLAN_DETAIL_temp CY
 on (ED.ACCOUNT_ID=CY.ACCOUNT_ID
 AND ED.PRODUCT_ID=CY.PRODUCT_ID
 and ED.COUNTRY_ID=CY.COUNTRY_ID)

WHERE C.CALL_STATUS = 'Submitted_vod' AND 
(C.CALL_TYPE LIKE 'F2F%' OR RT.RECORD_TYPE_NAME='Remote Meeting')
AND C.ISDELETED=0 AND ED.ISDELETED=0 AND cast(months_between (current_date(),dc.calen_DT) as int ) <=12  
AND CL_DOC.DOCUMENT_NUMBER IS NOT NULL
""")
dfEdetail.createOrReplaceTempView("F_CER_EDETAIL")
dfEdetail.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_EDETAIL")



# COMMAND ----------

# DBTITLE 1,EDETAIL_INTERMEDIATE1
dfCallSlideCount = spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
,A.CALL_ID
,cast(COUNT(DISTINCT A.CALL_ID,A.KEY_MESSAGE_ID) as int ) AS SLIDE_COUNT_PER_CUSTOMER_CALL
--AVG(KEY_MESSAGE_ID) as SLIDE_COUNT
FROM MOCA.F_CER_EDETAIL  A
LEFT OUTER JOIN MOCA.D_CRM_CALL B
ON A.CALL_ID = B.CALL_ID


WHERE
--DOCUMENT_NUMBER='PM-CA-MPL-DTLA-190001' AND
--(B.CLM_IND='true' AND B.CALL_STATUS = 'Submitted_vod' AND B.CALL_TYPE LIKE 'F2F%') AND --Change done one 04-Nov-2020 due to def change
B.CLM_IND='true' AND
 A.DURATION>0
GROUP BY A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
,A.CALL_ID
""")
dfCallSlideCount.createOrReplaceTempView("CALL_SLIDE_COUNT")


# COMMAND ----------

# DBTITLE 1,EDETAIL_INTERMEDIATE2
dfslideperuser = spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,COUNT(DISTINCT A.CALL_ID,A.KEY_MESSAGE_ID) AS TOTAL_SLIDES_PER_DOC
FROM MOCA.F_CER_EDETAIL A
LEFT OUTER JOIN MOCA.D_CRM_CALL B
ON A.CALL_ID = B.CALL_ID
WHERE
--(B.CLM_IND='true' AND B.CALL_STATUS = 'Submitted_vod' AND B.CALL_TYPE LIKE 'F2F%') --Change done one 04-Nov-2020 due to def change
B.CLM_IND='true'
GROUP BY 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
""")
dfslideperuser.createOrReplaceTempView("SLIDES_PER_DOC")


dfCallsPerUser = spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,COUNT(DISTINCT A.CALL_ID) AS TOTAL_CALLS_PER_DOC
FROM MOCA.F_CER_EDETAIL A
LEFT OUTER JOIN MOCA.D_CRM_CALL B
ON A.CALL_ID = B.CALL_ID
WHERE
--(B.CLM_IND='true' AND B.CALL_STATUS = 'Submitted_vod' AND B.CALL_TYPE LIKE 'F2F%') 
B.CLM_IND='true'
GROUP BY A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
""")
dfCallsPerUser.createOrReplaceTempView("CALLS_PER_DOC")

dfSlideAveragePerCustomer = spark.sql("""
SELECT
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
--,A.DATE_KEY
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
--,A.SOURCE_GIGYA_AID
,cast ((A.TOTAL_SLIDES_PER_DOC/B.TOTAL_CALLS_PER_DOC) as decimal (4,2)) AS AVERAGE_PER_DOC
FROM SLIDES_PER_DOC A
INNER JOIN
CALLS_PER_DOC B ON
--A.SOURCE_GIGYA_AID = B.SOURCE_GIGYA_AID
A.COUNTRY_ID = B.COUNTRY_ID AND
A.CALEN_MO= B.CALEN_MO AND
A.CALEN_YEAR= B.CALEN_YEAR AND
--A.DATE_KEY= B.DATE_KEY AND
A.DOCUMENT_ID = B.DOCUMENT_ID AND
--A.DOCUMENT_NUMBER = B.DOCUMENT_NUMBER AND -commented on 28th October
A.CAMPAIGN_ID = B.CAMPAIGN_ID   AND
A.GLOBAL_PRODUCT_ID = B.GLOBAL_PRODUCT_ID
""")
dfSlideAveragePerCustomer.createOrReplaceTempView("AVERAGE_SLIDES_PER_USER")

# COMMAND ----------

# DBTITLE 1,EDETAIL_INTERMEDIATE3
dfCustomerGreaterThanAverage = spark.sql("""
SELECT 
A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
--,A.DATE_KEY
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID,
A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
--A.CALL_ID
FROM CALL_SLIDE_COUNT A
INNER JOIN AVERAGE_SLIDES_PER_USER B ON
A.COUNTRY_ID = B.COUNTRY_ID
AND A.CALEN_MO= B.CALEN_MO
AND A.DOCUMENT_ID = B.DOCUMENT_ID
--AND A.DOCUMENT_NUMBER = B.DOCUMENT_NUMBER --commented on 28th Oct
AND A.CAMPAIGN_ID = B.CAMPAIGN_ID  
AND A.GLOBAL_PRODUCT_ID = B.GLOBAL_PRODUCT_ID
WHERE A.SLIDE_COUNT_PER_CUSTOMER_CALL >= B.AVERAGE_PER_DOC
""")
dfCustomerGreaterThanAverage.createOrReplaceTempView("CUSTOMER_GREATER_THAN_AVERAGE")
dfCustomerGreaterThanAverage.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.CUSTOMER_GREATER_THAN_AVERAGE")

# COMMAND ----------

# DBTITLE 1,EDETAIL INTERMEDIATE Step 4 - Average Slides per HCP
dfslideperhcp = spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
,COUNT(DISTINCT A.CALL_ID,A.KEY_MESSAGE_ID) AS TOTAL_SLIDES_PER_HCP
FROM MOCA.F_CER_EDETAIL A
LEFT OUTER JOIN MOCA.D_CRM_CALL B
ON A.CALL_ID = B.CALL_ID
WHERE  
B.CLM_IND='true'
GROUP BY A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
""")
dfslideperhcp.createOrReplaceTempView("SLIDES_PER_HCP")


dfCallsPerhcp = spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
,COUNT(DISTINCT A.CALL_ID) AS TOTAL_CALLS_PER_HCP
FROM MOCA.F_CER_EDETAIL A
INNER JOIN MOCA.D_CRM_CALL B
ON A.CALL_ID = B.CALL_ID
WHERE B.CLM_IND = 'true'
GROUP BY A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
""")
dfCallsPerhcp.createOrReplaceTempView("CALLS_PER_HCP")


dfSlideAveragePerhcp = spark.sql("""
SELECT
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.CAMPAIGN_ID
,A.GLOBAL_PRODUCT_ID
,A.SOURCE_GIGYA_AID
,A.CUSTOMER_ID
,A.VEEVA_ID
,A.VISITOR_ID
,(A.TOTAL_SLIDES_PER_HCP/B.TOTAL_CALLS_PER_HCP) AS AVERAGE_PER_HCP
FROM SLIDES_PER_HCP A
INNER JOIN
CALLS_PER_HCP B ON
--A.SOURCE_GIGYA_AID = B.SOURCE_GIGYA_AID AND
A.COUNTRY_ID = B.COUNTRY_ID AND
A.CALEN_MO= B.CALEN_MO AND
A.CALEN_YEAR= B.CALEN_YEAR AND
A.DOCUMENT_ID = B.DOCUMENT_ID AND
--A.DOCUMENT_NUMBER = B.DOCUMENT_NUMBER AND
--A.CAMPAIGN_ID = B.CAMPAIGN_ID   AND
A.GLOBAL_PRODUCT_ID = B.GLOBAL_PRODUCT_ID AND 
A.CUSTOMER_ID  = B.CUSTOMER_ID 
--AND A.VEEVA_ID     = B.VEEVA_ID AND
--A.VISITOR_ID   = B.VISITOR_ID 
""")



dfSlideAveragePerhcp.createOrReplaceTempView("AVERAGE_SLIDES_PER_HCP")


# COMMAND ----------

# DBTITLE 1,F_CER_Final by Eshita
dfEdetailFinal = spark.sql("""
SELECT 
ED.SOURCE_GIGYA_AID
,ED.CUSTOMER_ID
,ED.VEEVA_ID
,ED.VISITOR_ID
,ED.ACCOUNT_ID
,ED.ACCOUNT_SPECIALTY
,ED.SEGMENT
,ED.COUNTRY_ID
,ED.CALEN_MO
,ED.CALEN_YEAR
--,ED.DATE_KEY
,'NA' as CAMPAIGN_SUBJECT_LINE
,ED.CAMPAIGN_ID
,ED.CAMPAIGN_FULL_NAME
,ED.DOCUMENT_ID
,ED.DOCUMENT_NUMBER
,ED.GLOBAL_PRODUCT_ID
,1 AS EDETAIL_REACH_INCLUSIVE_ALL_CALLS
,CASE WHEN ED1.CUSTOMER_ID IS NOT NULL THEN 1 ELSE 0 END AS EDETAIL_REACH
,CASE WHEN CED.CUSTOMER_ID IS NULL THEN 0 ELSE 1 END AS EDETAIL_ENGAGE
,ASH.AVERAGE_PER_HCP AS AVERAGE_SLIDES_PER_EDETAIL_CALLS
,max(ED.TARGET_FLAG) as TARGET_FLAG

FROM MOCA.F_CER_EDETAIL ED
LEFT OUTER JOIN (
SELECT DISTINCT A.SOURCE_GIGYA_AID ,A.CUSTOMER_ID,A.VEEVA_ID,A.VISITOR_ID,A.COUNTRY_ID,A.CALEN_MO,A.CAMPAIGN_ID,A.DOCUMENT_ID,A.GLOBAL_PRODUCT_ID
,A.CALEN_MO,A.CALEN_YEAR

FROM MOCA.F_CER_EDETAIL  A
LEFT OUTER JOIN MOCA.D_CRM_CALL B ON A.CALL_ID = B.CALL_ID
WHERE B.CLM_IND='true'
) as ED1 ON
--on ED.SOURCE_GIGYA_AID=ED1.SOURCE_GIGYA_AID AND
 ED.CUSTOMER_ID = ED1.CUSTOMER_ID
AND ED.VEEVA_ID = ED1.VEEVA_ID
AND ED.VISITOR_ID = ED1.VISITOR_ID
AND ED.COUNTRY_ID=ED1.COUNTRY_ID
AND ED.CALEN_MO=ED1.CALEN_MO
AND ED.CALEN_YEAR=ED1.CALEN_YEAR
AND ED.CAMPAIGN_ID=ED1.CAMPAIGN_ID
AND ED.DOCUMENT_ID=ED1.DOCUMENT_ID
--AND ED.DOCUMENT_NUMBER=ED1.DOCUMENT_NUMBER
AND ED.GLOBAL_PRODUCT_ID=ED1.GLOBAL_PRODUCT_ID


LEFT OUTER JOIN CUSTOMER_GREATER_THAN_AVERAGE CED
ON 
--ED.SOURCE_GIGYA_AID=CED.SOURCE_GIGYA_AID AND
 ED.CUSTOMER_ID = CED.CUSTOMER_ID
AND ED.VEEVA_ID = CED.VEEVA_ID
AND ED.VISITOR_ID = CED.VISITOR_ID
AND ED.COUNTRY_ID=CED.COUNTRY_ID
AND ED.CALEN_MO=CED.CALEN_MO
AND ED.CALEN_YEAR=CED.CALEN_YEAR
--AND ED.DATE_KEY = CED.DATE_KEY
AND ED.CAMPAIGN_ID=CED.CAMPAIGN_ID
AND ED.DOCUMENT_ID=CED.DOCUMENT_ID
--AND ED.DOCUMENT_NUMBER=CED.DOCUMENT_NUMBER
AND ED.GLOBAL_PRODUCT_ID=CED.GLOBAL_PRODUCT_ID

LEFT OUTER JOIN AVERAGE_SLIDES_PER_HCP ASH ON 
--CED.SOURCE_GIGYA_AID=ASH.SOURCE_GIGYA_AID 
 ED.CUSTOMER_ID = ASH.CUSTOMER_ID
AND ED.VEEVA_ID = ASH.VEEVA_ID
AND ED.VISITOR_ID = ASH.VISITOR_ID
AND ED.COUNTRY_ID=ASH.COUNTRY_ID
AND ED.CALEN_YEAR=ASH.CALEN_YEAR
AND ED.CALEN_MO=ASH.CALEN_MO
AND ED.CAMPAIGN_ID=ASH.CAMPAIGN_ID
AND ED.DOCUMENT_ID=ASH.DOCUMENT_ID
--AND ED.DOCUMENT_NUMBER=ASH.DOCUMENT_NUMBER
AND ED.GLOBAL_PRODUCT_ID=ASH.GLOBAL_PRODUCT_ID




GROUP BY
ED.SOURCE_GIGYA_AID
,ED.CUSTOMER_ID
,ED.VEEVA_ID
,ED.VISITOR_ID
,ED.ACCOUNT_ID
,ED.ACCOUNT_SPECIALTY
,ED.SEGMENT
,ED.COUNTRY_ID
,ED.CALEN_MO
,ED.CALEN_YEAR
,ED.CAMPAIGN_ID
,ED.CAMPAIGN_FULL_NAME
,ED.DOCUMENT_ID
,ED.DOCUMENT_NUMBER
,ED.GLOBAL_PRODUCT_ID
,CED.CUSTOMER_ID
,ED1.CUSTOMER_ID
,ASH.AVERAGE_PER_HCP
""")

#dfEdetailFinal.createOrReplaceTempView("F_CER_EDETAIL_FINAL")
dfEdetailFinal.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_EDETAIL_FINAL")


# COMMAND ----------

# DBTITLE 1,Wide Table: combination of campaign and mass email-- without aggregation
sqlContext.setConf("spark.driver.maxResultSize","10M")
#sqlContext.getConf("spark.driver.maxResultSize")
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
dfWideWithOutAggr=spark.sql(""" 
select  
c.SOURCE_GIGYA_AID
,c.CUSTOMER_ID
,c.VEEVA_ID
,c.VISITOR_ID
,c.ACCOUNT_ID
,c.ACCOUNT_SPECIALTY
,c.SEGMENT
,c.COUNTRY_ID
,c.CALEN_MO
,c.CALEN_YEAR
,c.CAMPAIGN_ID
,c.CAMPAIGN_FULL_NAME
,c.DOCUMENT_ID
,c.DOCUMENT_NUMBER
,c.CAMPAIGN_SUBJECT_LINE
--,ED.CL_BRAND_ID
,c.GLOBAL_PRODUCT_ID
,c.MASS_EMAIL_OPENED
,c.MASS_EMAIL_CLICKED
,c.MASS_EMAIL_DELIVERED
,c.ONETOONEEMAIL_CLICKED
,c.ONETOONEEMAIL_OPENED
,c.ONETOONEEMAIL_DELIVERED
,c.PORTAL_REACH
,c.PORTAL_ENGAGE
,c.PORTAL_VISITS
,c.EDETAIL_REACH_INCLUSIVE_ALL_CALLS
,c.EDETAIL_REACH
,c.EDETAIL_ENGAGE
,c.AVERAGE_SLIDES_PER_EDETAIL_CALLS
,DCT.DOCUMENT_TYPE_NAME
,DCD.DOCUMENT_NAME
,FDS.CONTENT_PURPOSE
,c.TARGET_FLAG
 from(

select  
CASE  WHEN b.SOURCE_GIGYA_AID IS NULL THEN ED.SOURCE_GIGYA_AID ELSE b.SOURCE_GIGYA_AID END AS SOURCE_GIGYA_AID
,CASE WHEN b.CUSTOMER_ID IS NULL THEN ED.CUSTOMER_ID ELSE b.CUSTOMER_ID END AS CUSTOMER_ID
, CASE WHEN b.VEEVA_ID IS NULL THEN ED.VEEVA_ID ELSE b.VEEVA_ID END AS VEEVA_ID
, CASE WHEN b.VISITOR_ID IS NULL THEN ED.VISITOR_ID ELSE b.VISITOR_ID END AS VISITOR_ID
,CASE WHEN b.ACCOUNT_ID IS NULL THEN ED.ACCOUNT_ID ELSE b.ACCOUNT_ID END AS ACCOUNT_ID
,CASE WHEN b.ACCOUNT_SPECIALTY IS NULL THEN ED.ACCOUNT_SPECIALTY ELSE b.ACCOUNT_SPECIALTY END AS ACCOUNT_SPECIALTY
,CASE WHEN b.SEGMENT IS NULL THEN ED.SEGMENT  ELSE b.SEGMENT end AS SEGMENT
,cast(CASE WHEN b.COUNTRY_ID IS NULL THEN ED.COUNTRY_ID ELSE b.COUNTRY_ID END as int) AS COUNTRY_ID
,cast(CASE WHEN b.CALEN_MO IS NULL THEN ED.CALEN_MO ELSE b.CALEN_MO END as int) AS CALEN_MO
,cast(CASE WHEN b.CALEN_YEAR IS NULL THEN ED.CALEN_YEAR ELSE b.CALEN_YEAR END as int) AS CALEN_YEAR
--,cast(CASE WHEN b.DATE_KEY IS NULL THEN ED.DATE_KEY ELSE b.DATE_KEY END as int) AS DATE_KEY
,CASE WHEN b.CAMPAIGN_ID IS NULL THEN ED.CAMPAIGN_ID ELSE b.CAMPAIGN_ID END  AS CAMPAIGN_ID
,CASE WHEN b.CAMPAIGN_FULL_NAME IS NULL THEN ED.CAMPAIGN_FULL_NAME ELSE b.CAMPAIGN_FULL_NAME END  AS CAMPAIGN_FULL_NAME

,cast(CASE WHEN b.DOCUMENT_ID IS NULL THEN ED.DOCUMENT_ID ELSE b.DOCUMENT_ID END as int) AS DOCUMENT_ID
,CASE WHEN b.DOCUMENT_NUMBER IS NULL THEN ED.DOCUMENT_NUMBER ELSE b.DOCUMENT_NUMBER END AS DOCUMENT_NUMBER
,case WHEN b.CAMPAIGN_SUBJECT_LINE is NULL THEN ED.CAMPAIGN_SUBJECT_LINE else b.CAMPAIGN_SUBJECT_LINE end AS CAMPAIGN_SUBJECT_LINE
--,ED.CL_BRAND_ID
,cast(CASE WHEN b.GLOBAL_PRODUCT_ID IS NULL THEN ED.GLOBAL_PRODUCT_ID ELSE b.GLOBAL_PRODUCT_ID END as int) AS GLOBAL_PRODUCT_ID
,b.MASS_EMAIL_OPENED
,b.MASS_EMAIL_CLICKED
,b.MASS_EMAIL_DELIVERED
,b.ONETOONEEMAIL_CLICKED
,b.ONETOONEEMAIL_OPENED
,b.ONETOONEEMAIL_DELIVERED
,b.PORTAL_REACH
,b.PORTAL_ENGAGE
,b.PORTAL_VISITS
,ED.EDETAIL_REACH
,ED.EDETAIL_ENGAGE
,ED.AVERAGE_SLIDES_PER_EDETAIL_CALLS
,ED.EDETAIL_REACH_INCLUSIVE_ALL_CALLS
,NVL(b.TARGET_FLAG,ED.TARGET_FLAG) as TARGET_FLAG
 from(

select  
CASE WHEN a.SOURCE_GIGYA_AID IS NULL THEN p.SOURCE_GIGYA_AID ELSE a.SOURCE_GIGYA_AID END AS SOURCE_GIGYA_AID
,CASE WHEN a.CUSTOMER_ID IS NULL THEN p.CUSTOMER_ID ELSE a.CUSTOMER_ID END AS CUSTOMER_ID
, CASE WHEN a.VEEVA_ID IS NULL THEN p.VEEVA_ID ELSE a.VEEVA_ID END AS VEEVA_ID
, CASE WHEN a.VISITOR_ID IS NULL THEN p.VISITOR_ID ELSE a.VISITOR_ID END AS VISITOR_ID
,CASE WHEN a.ACCOUNT_ID IS NULL THEN p.ACCOUNT_ID ELSE a.ACCOUNT_ID END AS ACCOUNT_ID
,CASE WHEN a.ACCOUNT_SPECIALTY IS NULL THEN p.ACCOUNT_SPECIALTY ELSE a.ACCOUNT_SPECIALTY END AS ACCOUNT_SPECIALTY
,CASE WHEN a.SEGMENT IS NULL THEN p.SEGMENT  ELSE a.SEGMENT end AS SEGMENT
,cast(CASE WHEN a.COUNTRY_ID IS NULL THEN p.COUNTRY_ID ELSE a.COUNTRY_ID END as int) AS COUNTRY_ID
,cast(CASE WHEN a.CALEN_MO IS NULL THEN p.CALEN_MO ELSE a.CALEN_MO END as int) AS CALEN_MO
,cast(CASE WHEN a.CALEN_YEAR IS NULL THEN p.CALEN_YEAR ELSE a.CALEN_YEAR END as int) AS CALEN_YEAR
--,cast(CASE WHEN a.DATE_KEY IS NULL THEN p.DATE_KEY ELSE a.DATE_KEY END as string) AS DATE_KEY
,CASE WHEN a.CAMPAIGN_ID IS NULL THEN p.CAMPAIGN_ID ELSE a.CAMPAIGN_ID END  AS CAMPAIGN_ID
,CASE WHEN a.CAMPAIGN_FULL_NAME IS NULL THEN p.CAMPAIGN_FULL_NAME ELSE a.CAMPAIGN_FULL_NAME END  AS CAMPAIGN_FULL_NAME
,cast(CASE WHEN a.DOCUMENT_ID IS NULL THEN p.DOCUMENT_ID ELSE a.DOCUMENT_ID END as int) AS DOCUMENT_ID
,CASE WHEN a.DOCUMENT_NUMBER IS NULL THEN p.DOCUMENT_NUMBER ELSE a.DOCUMENT_NUMBER END AS DOCUMENT_NUMBER
,a.CAMPAIGN_SUBJECT_LINE AS CAMPAIGN_SUBJECT_LINE
--,p.CL_BRAND_ID
,cast(CASE WHEN a.GLOBAL_PRODUCT_ID IS NULL THEN p.GLOBAL_PRODUCT_ID ELSE a.GLOBAL_PRODUCT_ID END as int) AS GLOBAL_PRODUCT_ID
,a.MASS_EMAIL_OPENED
,a.MASS_EMAIL_CLICKED
,a.MASS_EMAIL_DELIVERED
,a.ONETOONEEMAIL_CLICKED
,a.ONETOONEEMAIL_OPENED
,a.ONETOONEEMAIL_DELIVERED
,p.PORTAL_REACH
,p.PORTAL_ENGAGE
,p.PORTAL_VISITS
,NVL(a.TARGET_FLAG,'NA') as TARGET_FLAG

from (
select CASE WHEN EA.SOURCE_GIGYA_AID IS NULL THEN FM.SOURCE_GIGYA_AID ELSE EA.SOURCE_GIGYA_AID END AS SOURCE_GIGYA_AID
, CASE WHEN EA.CUSTOMER_ID IS NULL THEN FM.CUSTOMER_ID ELSE EA.CUSTOMER_ID END AS CUSTOMER_ID
, CASE WHEN EA.VEEVA_ID IS NULL THEN FM.VEEVA_ID ELSE EA.VEEVA_ID END AS VEEVA_ID
, CASE WHEN EA.VISITOR_ID IS NULL THEN FM.VISITOR_ID ELSE EA.VISITOR_ID END AS VISITOR_ID
,CASE WHEN EA.ACCOUNT_ID IS NULL THEN FM.ACCOUNT_ID ELSE EA.ACCOUNT_ID END AS ACCOUNT_ID
,CASE WHEN EA.ACCOUNT_SPECIALTY IS NULL THEN FM.ACCOUNT_SPECIALTY ELSE EA.ACCOUNT_SPECIALTY END AS ACCOUNT_SPECIALTY
,CASE WHEN EA.SEGMENT IS NULL THEN FM.SEGMENT  ELSE EA.SEGMENT end AS SEGMENT
,cast(CASE WHEN EA.COUNTRY_ID IS NULL THEN FM.COUNTRY_ID ELSE EA.COUNTRY_ID END as int) AS COUNTRY_ID
,cast(CASE WHEN EA.CALEN_MO IS NULL THEN FM.CALEN_MO ELSE EA.CALEN_MO END as int) AS CALEN_MO
,cast(CASE WHEN EA.CALEN_YEAR IS NULL THEN FM.CALEN_YEAR ELSE EA.CALEN_YEAR END as int) AS CALEN_YEAR
--,cast(CASE WHEN EA.DATE_KEY IS NULL THEN FM.DATE_KEY ELSE EA.DATE_KEY END as string) AS DATE_KEY
,CASE WHEN EA.CAMPAIGN_ID IS NULL THEN FM.CAMPAIGN_ID ELSE EA.CAMPAIGN_ID END  AS CAMPAIGN_ID
,CASE WHEN EA.CAMPAIGN_FULL_NAME IS NULL THEN FM.CAMPAIGN_FULL_NAME ELSE EA.CAMPAIGN_FULL_NAME END  AS CAMPAIGN_FULL_NAME

,cast(CASE WHEN EA.DOCUMENT_ID IS NULL THEN FM.DOCUMENT_ID ELSE EA.DOCUMENT_ID END as int) AS DOCUMENT_ID
,CASE WHEN EA.DOCUMENT_NUMBER IS NULL THEN FM.DOCUMENT_NUMBER ELSE EA.DOCUMENT_NUMBER END AS DOCUMENT_NUMBER
,CASE WHEN EA.CAMP_SUBJECT IS NULL THEN FM.EMAIL_SUBJECT ELSE EA.CAMP_SUBJECT END AS CAMPAIGN_SUBJECT_LINE
--,FM.CL_BRAND_ID
,cast(CASE WHEN EA.GLOBAL_PRODUCT_ID IS NULL THEN FM.GLOBAL_PRODUCT_ID ELSE EA.GLOBAL_PRODUCT_ID END as int) AS GLOBAL_PRODUCT_ID
,EA.EMAIL_OPENED AS MASS_EMAIL_OPENED
,EA.EMAIL_CLICKED AS MASS_EMAIL_CLICKED
,EA.EMAIL_DELIVERED AS MASS_EMAIL_DELIVERED
, CLICKED_EMAIL AS ONETOONEEMAIL_CLICKED
, OPENED_EMAIL AS ONETOONEEMAIL_OPENED
,DELIVERED_EMAIL AS ONETOONEEMAIL_DELIVERED
,NVL(FM.TARGET_FLAG,'NA') as TARGET_FLAG



 FROM MOCA.F_CER_EMAIL_ACTIVITY_ACCOUNT_MONTHLY FM
FULL OUTER JOIN MOCA.F_CER_ACCOUNT_MESSAGE_MONTHLY_WIDE EA
ON EA.SOURCE_GIGYA_AID=FM.SOURCE_GIGYA_AID
AND EA.ACCOUNT_ID=FM.ACCOUNT_ID
AND EA.SEGMENT=FM.SEGMENT
AND EA.COUNTRY_ID=FM.COUNTRY_ID
AND EA.CALEN_MO=FM.CALEN_MO
AND EA.CAMPAIGN_ID=FM.CAMPAIGN_ID
AND EA.DOCUMENT_ID=FM.DOCUMENT_ID
AND EA.DOCUMENT_NUMBER=FM.DOCUMENT_NUMBER
AND EA.GLOBAL_PRODUCT_ID=FM.GLOBAL_PRODUCT_ID) a

FULL OUTER JOIN MOCA.F_CER_PORTAL p
ON a.SOURCE_GIGYA_AID=p.SOURCE_GIGYA_AID
AND a.ACCOUNT_ID=p.ACCOUNT_ID
AND a.SEGMENT=p.SEGMENT
AND a.COUNTRY_ID=p.COUNTRY_ID
AND a.CALEN_MO=p.CALEN_MO
AND a.CAMPAIGN_ID=p.CAMPAIGN_ID
AND a.DOCUMENT_ID=p.DOCUMENT_ID
AND a.DOCUMENT_NUMBER=p.DOCUMENT_NUMBER
AND a.GLOBAL_PRODUCT_ID=p.GLOBAL_PRODUCT_ID )b

FULL OUTER JOIN MOCA.F_CER_EDETAIL_FINAL ED
ON  b.SOURCE_GIGYA_AID=ED.SOURCE_GIGYA_AID
AND b.ACCOUNT_ID=ED.ACCOUNT_ID
AND b.SEGMENT=ED.SEGMENT
AND b.COUNTRY_ID=ED.COUNTRY_ID
AND b.CALEN_MO=ED.CALEN_MO
AND b.CAMPAIGN_ID=ED.CAMPAIGN_ID
AND b.DOCUMENT_ID=ED.DOCUMENT_ID
AND b.DOCUMENT_NUMBER=ED.DOCUMENT_NUMBER
AND b.GLOBAL_PRODUCT_ID=ED.GLOBAL_PRODUCT_ID ) c

LEFT OUTER JOIN MOCA.D_CL_DOCUMENT DCD
ON  c.DOCUMENT_NUMBER = DCD.DOCUMENT_NUMBER 
LEFT OUTER JOIN 
(SELECT DOCUMENT_NUMBER,DOCUMENT_TYPE_ID,CONTENT_PURPOSE FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
GROUP BY DOCUMENT_NUMBER, DOCUMENT_TYPE_ID,CONTENT_PURPOSE) FDS
ON c.DOCUMENT_NUMBER = FDS.DOCUMENT_NUMBER 

--LEFT OUTER JOIN MOCA.D_CL_CAMPAIGN CN on (c.CAMPAIGN_ID =CN.CAMPAIGN_NAME or c.CAMPAIGN_ID='NA')

--LEFT OUTER JOIN (SELECT CAMPAIGN_ID,CONTENT_PURPOSE FROM MOCA.F_DOCUMENTS_STG  WHERE REC_RK=1 
--GROUP BY CAMPAIGN_ID,CONTENT_PURPOSE) CONTENT
--ON CN.CAMPAIGN_ID=CONTENT.CAMPAIGN_ID

LEFT OUTER JOIN MOCA.D_CL_DOCUMENT_TYPE DCT
ON FDS.DOCUMENT_TYPE_ID = DCT.DOCUMENT_TYPE_ID


""")

#dfWideWithOutAggr.createOrReplaceTempView("F_CER_WIDE_FINAL_TEST")

dfWideWithOutAggr.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("MOCA.F_CER_WIDE_FINAL")
#ExecuteSQLDWLoadMain("F_CER_WIDE_FINAL","Y")

# COMMAND ----------

# DBTITLE 1,MOCA.F_CER_WIDE
sqlContext.setConf("spark.driver.maxResultSize","60M")
sqlContext.setConf("bulkCopyTableLock","false")
sqlContext.setConf("bulkCopyBatchSize","60M")
dfWideAggregation=spark.sql("""select SOURCE_GIGYA_AID,CUSTOMER_ID
,ACCOUNT_ID
,ACCOUNT_SPECIALTY
,COUNTRY_ID
,VEEVA_ID
,VISITOR_ID
,CALEN_MO
,CALEN_YEAR
,DATE_KEY
,CASE WHEN CAMPAIGN_ID ='NA' THEN NULL ELSE CAMPAIGN_ID END AS CAMPAIGN_ID
,CAMPAIGN_FULL_NAME
,DOCUMENT_ID
,DOCUMENT_NUMBER
,CAMPAIGN_SUBJECT_LINE
,SEGMENT
,GLOBAL_PRODUCT_ID
,DOCUMENT_NAME
,DOCUMENT_TYPE_NAME
,CONTENT_PURPOSE
,EDETAIL_REACH_INCLUSIVE_ALL_CALLS
,AVERAGE_SLIDES_PER_EDETAIL_CALLS
,MASS_EMAIL_REACH,MASS_EMAIL_ENGAGE,MASS_EMAIL_DELIVERED
,ONETOONE_EMAIL_REACH,ONETOONE_EMAIL_ENGAGE,ONETOONE_EMAIL_DELIVERED,PORTAL_REACH,PORTAL_ENGAGE
,PORTAL_VISITS
,EDETAIL_REACH
,EDETAIL_ENGAGE
,case when (MASS_EMAIL_REACH+ONETOONE_EMAIL_REACH+PORTAL_REACH+EDETAIL_REACH)>0 then 1 else 0  end as TotalUniqueReach
,case when (MASS_EMAIL_ENGAGE+ONETOONE_EMAIL_ENGAGE+PORTAL_ENGAGE+EDETAIL_ENGAGE)>0 then 1  else 0 end  as TotalUniqueEngage
,case when TARGET_FLAG==1 then 'Y' when TARGET_FLAG==0 THEN 'N' else TARGET_FLAG end as TARGET_FLAG
from
(select
SOURCE_GIGYA_AID,CUSTOMER_ID
,ACCOUNT_ID
,ACCOUNT_SPECIALTY
,COUNTRY_ID
,CALEN_MO
,CALEN_YEAR
,concat_ws("_",cast(CALEN_MO as string),cast(CALEN_YEAR as string) ) as DATE_KEY
,CAMPAIGN_ID
,CAMPAIGN_FULL_NAME
,VEEVA_ID
,VISITOR_ID
,DOCUMENT_ID
,DOCUMENT_NUMBER
,CONTENT_PURPOSE
,CAMPAIGN_SUBJECT_LINE
,SEGMENT
,GLOBAL_PRODUCT_ID
,case when max(MASS_EMAIL_OPENED)>0 then 1 else 0 end AS MASS_EMAIL_REACH
,case when max(MASS_EMAIL_CLICKED)>0 then 1 else 0 end AS MASS_EMAIL_ENGAGE
,case when max(MASS_EMAIL_DELIVERED)>0 then 1 else 0 end AS MASS_EMAIL_DELIVERED
,case when max(ONETOONEEMAIL_CLICKED)>0 then 1 else 0 end AS ONETOONE_EMAIL_ENGAGE 
,case when max(ONETOONEEMAIL_OPENED)>0 then 1 else 0 end  AS ONETOONE_EMAIL_REACH
,case when max(ONETOONEEMAIL_DELIVERED)>0 then 1 else 0 end  AS ONETOONE_EMAIL_DELIVERED
,max(nvl(PORTAL_REACH,0)) AS PORTAL_REACH
,max(nvl(PORTAL_ENGAGE,0)) AS PORTAL_ENGAGE
,max(nvl(PORTAL_VISITS,0)) AS PORTAL_VISITS
,max(nvl(EDETAIL_REACH,0)) AS EDETAIL_REACH
,max(nvl(EDETAIL_ENGAGE,0)) AS EDETAIL_ENGAGE
--,max(TotalUniqueEngage) as TotalUniqueEngage
--,max(TotalUniqueReach) as TotalUniqueReach
,DOCUMENT_NAME
,DOCUMENT_TYPE_NAME
,AVERAGE_SLIDES_PER_EDETAIL_CALLS
,MAX(EDETAIL_REACH_INCLUSIVE_ALL_CALLS) AS EDETAIL_REACH_INCLUSIVE_ALL_CALLS
,MAX(TARGET_FLAG) as TARGET_FLAG

 FROM MOCA.F_CER_WIDE_FINAL EA
 --LEFT OUTER JOIN TotalUniqueOrReach T on EA.SOURCE_GIGYA_AID=T.SOURCE_GIGYA_AID
 where EA.CUSTOMER_ID not in ('<%','987654321','test')
GROUP BY
EA.SOURCE_GIGYA_AID,CUSTOMER_ID
,ACCOUNT_ID
,ACCOUNT_SPECIALTY
,VEEVA_ID
,VISITOR_ID
,COUNTRY_ID
,CALEN_MO
,CALEN_YEAR
,concat_ws("_",cast(CALEN_MO as string),cast(CALEN_YEAR as string) )
,CAMPAIGN_ID
,CAMPAIGN_FULL_NAME
,DOCUMENT_ID
,DOCUMENT_NUMBER
,CAMPAIGN_SUBJECT_LINE
,SEGMENT
,GLOBAL_PRODUCT_ID
,DOCUMENT_NAME
,DOCUMENT_TYPE_NAME
,CONTENT_PURPOSE
,AVERAGE_SLIDES_PER_EDETAIL_CALLS) a """)

dfWideAggregation.createOrReplaceTempView("F_CER_WIDE")
#Remove all the records which has 0 value in reach and engage
#dfWide=spark.sql("""SELECT * FROM F_CER_WIDE
#EXCEPT
#SELECT * FROM F_CER_WIDE WHERE TotalUniqueEngage=0 AND TotalUniqueReach=0 """)
#dfWideAggregation.createOrReplaceTempView("F_CER_WIDE")
dfWideAggregation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_WIDE")
ExecuteSQLDWLoadMain("F_CER_WIDE","Y")



# COMMAND ----------

#ExecuteSQLDWLoadMain("F_CER_WIDE","Y")--57522049

# COMMAND ----------

# DBTITLE 1,Total Campaign
dfTotalCampaign=spark.sql("""select CAMP.CAMPAIGN_NAME AS CAMPAIGN_ID,
CAMP.CAMPAIGN_FULL_NAME,
CAMP.COUNTRY_ID,
CALEN.CALEN_MO AS CALEN_MO,
CALEN.CALEN_YEAR AS CALEN_YEAR,
CAMP.GLOBAL_PRODUCT_ID
FROM MOCA.D_CL_CAMPAIGN CAMP
INNER JOIN MOCA.D_CALENDAR CALEN
ON CAMP.START_DATE_ID = CALEN.PERD_ID """)
dfTotalCampaign.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CER_TOTAL_CAMPAIGN")
ExecuteSQLDWLoadMain("D_CER_TOTAL_CAMPAIGN","Y")

# COMMAND ----------

# DBTITLE 1,Total Docuement
dfTotalDocument=spark.sql("""select DOC.DOCUMENT_ID,
--DOC.DOCUMENT_NAME ,
DOC.DOCUMENT_NUMBER,
GP.GLOBAL_PRODUCT_ID,
--FD.COUNTRY_ID,
CALEN.CALEN_MO,
CALEN.CALEN_YEAR,
CON.COUNTRY_ID
FROM MOCA.D_CL_DOCUMENT DOC
INNER JOIN MOCA.D_CALENDAR CALEN
ON DOC.START_DATE_ID = CALEN.PERD_ID
INNER JOIN MOCA.F_DOCUMENTS_STG FD
ON DOC.DOCUMENT_ID = FD.DOCUMENT_ID
INNER JOIN MOCA.H_CL_DOC_GLOBAL_PRODUCT GP
ON FD.DOCUMENT_VERSION = GP.DOCUMENT_VERSION
INNER JOIN MOCA.D_COUNTRY CON
ON GP.COUNTRY = CON.CL_COUNTRY_NAME
where   FD.REC_RK=1 
""")
dfTotalDocument.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.D_CER_TOTAL_DOCUMENT")
ExecuteSQLDWLoadMain("D_CER_TOTAL_DOCUMENT","Y")

# COMMAND ----------

# DBTITLE 1,MOCA.F_CER_EDETAIL_CALL_SLIDES
dfEdetailslides=spark.sql("""
SELECT 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.CAMPAIGN_ID
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.GLOBAL_PRODUCT_ID
,A.CALL_ID
,count(distinct A.CALL_ID, A.KEY_MESSAGE_ID) AS SLIDE_COUNT
FROM MOCA.F_CER_EDETAIL  A
INNER JOIN MOCA.D_CRM_cALL B
ON A.CALL_ID=B.CALL_ID
WHERE B.CLM_IND='true'
GROUP BY 
 A.COUNTRY_ID
,A.CALEN_MO
,A.CALEN_YEAR
,A.CAMPAIGN_ID
,A.DOCUMENT_ID
,A.DOCUMENT_NUMBER
,A.GLOBAL_PRODUCT_ID
,A.CALL_ID
""")
dfEdetailslides.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.F_CER_EDETAIL_CALL_SLIDES")
ExecuteSQLDWLoadMain("F_CER_EDETAIL_CALL_SLIDES","Y")

# COMMAND ----------

# MAGIC %sql
# MAGIC select SUM(AVERAGE_SLIDES_PER_EDETAIL_CALLS) from moca.f_Cer_wide where Global_product_id=4 AND CALEN_MO=5 AND CALEN_YEAR=2020

# COMMAND ----------

# MAGIC %sql 
# MAGIC select campaign_id,document_version from moca.f_documents_stg where document_number in  ('PM-AR-MPL-EML-200019','PM-CH-FPS-EML-200012','PM-CY-MPL-WCNT-200005','PM-IE-FVU-EML-200042','PM-IN-ACA-EML-200045','PM-IN-CFA-EML-200043','PM-IN-MUP-EML-200019','PM-IN-MUP-EML-200018','PM-IN-MUP-EML-200021','PM-LK-SLB-EML-200001','PM-MX-PRX-EML-200025','PM-NG-ACA-EML-200033','PM-UA-FPS-EDTL-200003','PM-UA-FLP-EDTL-200002') AND REC_RK=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.cl_country_name,f.global_product_name,count(distinct a.key_message_id) as key_message_count from moca.d_crm_key_message a
# MAGIC     inner join moca.f_call_key_message b
# MAGIC     on a.key_message_id = b.key_message_id
# MAGIC     inner join MOCA.F_DOCUMENTS_STG c
# MAGIC     on a.document_id = c.document_id
# MAGIC     inner join moca.d_country d
# MAGIC     on d.country_id = b.country_id
# MAGIC     inner join moca.h_cl_doc_global_product e
# MAGIC     on c.document_version = e.document_version
# MAGIC     inner join moca.d_cl_global_product f
# MAGIC     on e.global_product_id = f.global_product_id
# MAGIC     where c.rec_rk=1
# MAGIC     --and  c.key_content_exception_indicator = ''
# MAGIC     and c.key_content='true'
# MAGIC     and d.aa_country_name in ('Brazil'
# MAGIC ,'Canada'
# MAGIC ,'China'
# MAGIC ,'France'
# MAGIC ,'Germany'
# MAGIC ,'Italy'
# MAGIC ,'Japan'
# MAGIC ,'Spain'
# MAGIC ,'United Kingdom'
# MAGIC ,'United States')
# MAGIC     group by d.cl_country_name,f.global_product_name
# MAGIC     order by d.cl_country_name,f.global_product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from F_CER_EDETAIL_TEST;