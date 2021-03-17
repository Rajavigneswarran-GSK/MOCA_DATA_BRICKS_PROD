# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text('SecretScope','')
dbutils.widgets.text("JdbcUrlKeyDW",'')
dbutils.widgets.text("BlobStorage",'')
dbutils.widgets.text("BlobAccessKey",'')

# COMMAND ----------


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

df=spark.read.csv("/mnt/root/Enriched/Common/Global/Veeva Promomat/edl_item__v/edl_item__v.csv",header=True)
df.createOrReplaceTempView("edl")
df=spark.read.csv("/mnt/root/Enriched/Common/Global/Veeva Promomat/matched_documents/matched_documents.csv",header=True)
df.createOrReplaceTempView("docs")
df=spark.read.csv("/mnt/root/config/usqlscripts/Veeva Promomat/Curated/f_documents.csv",header=True)
df.createOrReplaceTempView("documents")


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Row_number() over (Partition by matching_doc_id__v order by to_timestamp(Planned_Delivery_date__c,'dd-MM-yyyy'),to_timestamp(a.modified_date__v,'dd-MM-yyy') desc) as rnk ,a.id,name__v,completeness__v,planned_delivery_date__c ,replanned_delivery_date__c,a.modified_date__v
# MAGIC from edl a
# MAGIC left outer join docs b on a.id=b.edl_item_id__v
# MAGIC where b.matching_doc_id__v=293940

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from docs where edl_item_id__v ='0EI000000004015'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl where modified_date__v like '2021-02-20%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from documents where Source_document_AID = '915749'

# COMMAND ----------

# MAGIC %sql
# MAGIC  select distinct source_document_aid,Replanned_for_loc_delivery_date,planned_for_loc_delivery_date,document_number,
# MAGIC EXPECTED_DOCUMENT_LIST_ITEM_COMPLETION_STATUS,EXPECTED_DOCUMENT_LIST_ITEM_AID,document_version,
# MAGIC * from documents where
# MAGIC  expected_document_list_item_aid in ('0EI000000004E80')

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from docs

# COMMAND ----------

df=spark.read.csv("/mnt/root/Enriched/Common/Global/Veeva Promomat/workflows/workflows.csv",header=True)
df.createOrReplaceTempView("workflows")

# COMMAND ----------

# MAGIC %sql
# MAGIC select source_document_id

# COMMAND ----------

df=spark.read.csv("/mnt/root/Target/Raja/Workflows.csv",header=True)
df.createOrReplaceTempView("workflows1")

# COMMAND ----------

df=spark.read.csv("/mnt/root/Curated/Global/MOC Analytics/Veeva Promomat/f_documents_approval_workflow_task/f_documents_approval_workflow_task.csv",header=True)
df.createOrReplaceTempView("workflows2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workflows where workflow_document_id__v = 1371903 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workflows1 where workflow_document_id__v = 1384076

# COMMAND ----------

df=spark.read.csv("/mnt/root/Target/Raja/check1.csv",header=True)
df.createOrReplaceTempView("modifieddate")

# COMMAND ----------

# MAGIC %sql
# MAGIC select convert(date,convert(varchar(100),Modified_date,112)) as md,id from modifieddate 

# COMMAND ----------

df=spark.read.csv("/mnt/root/Target/Rahul/Content Production Source/Raw Data DC CSV.csv",header=True)
df.createOrReplaceTempView("Raw_Data_DC_CSV")

# COMMAND ----------

df=spark.read.csv("/mnt/root/Target/Rahul/Content Production Source/Raw Data DC CSV.csv",header=True)
df.createOrReplaceTempView("Raw_Data_DC_CSV")
df=spark.read.csv("/mnt/root/Target/Rahul/Content Production Source/CR DATA.csv",header=True)
df.createOrReplaceTempView("CR_DATA_CSV")
df=spark.read.csv("/mnt/root/Target/Rahul/Content Production Source/RAW DATA MOC.csv",header=True)
df.createOrReplaceTempView("RAW_DATA_MOC_CSV")

# COMMAND ----------

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


df=spark.sql("""
Select 
'REQUEST#',
Brand,
Franchise,
'Therapy Area',
Region,
'Channel Details',
Month,
Quarter,
'Adapted from Global',
'Hub/LOC',
'ff',
'Complexity Status',
'Country (PC-Data)',
'Country/Cluster',
Clusters,
'Pre prod_TAT_in_Days',
'Pre prod_TAT',
'D1 SLA',
'D1 SLA_Days',
Deply_TAT_in_Days,
Deply_TAT,
CR_TAT_in_Days,
CR_TAT,
'Change Rounds Count',
'Overall TAT%',
'Overall TAT days',
'UAT_Days'

From 
Raw_Data_DC_CSV
""")
df.createOrReplaceTempView("draw")


# COMMAND ----------


df=spark.sql("""
Select 
*
From 
Raw_Data_DC_CSV
""")
df.createOrReplaceTempView("RAW_DATA_DC")
df1.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.RAW_DATA_DC")



# COMMAND ----------


df=spark.sql("""
Select 
*
From 
CR_DATA_CSV
""")
df.createOrReplaceTempView("CR_DATA")
df1.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.CR_DATA")



# COMMAND ----------


df=spark.sql("""
Select 
*
From 
RAW_DATA_MOC_CSV
""")
df.createOrReplaceTempView("RAW_DATA_MOC")
df1.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.RAW_DATA_MOC")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from RAW_DATA_DC

# COMMAND ----------

#Function to rename the column names
def get_column_renamed(df):
  for col in df.columns:
    df=df.withColumnRenamed(col,col.replace(" ", "_").replace("-","_").replace("/","_").replace("o;?","").replace("+","").replace(";","").replace("?","").replace(",","").replace("{","").replace("}","").replace("(","").replace(")","").replace("\n","").replace("\t","").replace("=","").replace(".",""))
  return df

# COMMAND ----------

df1=get_column_renamed(df)

# COMMAND ----------

ExecuteSQLDWLoadMain("RAW_DATA_DC","Y")

# COMMAND ----------

ExecuteSQLDWLoadMain("CR_DATA","Y")


# COMMAND ----------

ExecuteSQLDWLoadMain("RAW_DATA_MOC","Y")
