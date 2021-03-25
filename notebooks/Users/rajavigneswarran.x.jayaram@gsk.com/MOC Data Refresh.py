# Databricks notebook source
# DBTITLE 1,Read File from path 
df=spark.read.csv("/mnt/root/Target/Rahul/Content Production MOCA Tables/Raw Data_MOC.csv",header=True)
df.createOrReplaceTempView("RAW_DATA_MOC_CSV")

# COMMAND ----------

# DBTITLE 1,Create it as temp view 

df=spark.sql("""
Select 
*
From 
RAW_DATA_MOC_CSV
""")
df.createOrReplaceTempView("RAW_DATA_MOC")

# COMMAND ----------

# DBTITLE 1,Renaming the columns
#Function to rename the column names
def get_column_renamed(df):
  for col in df.columns:
    df=df.withColumnRenamed(col,col.replace(" ", "_").replace("-","_").replace("/","_").replace("o;?","").replace("+","").replace(";","").replace("?","").replace(",","").replace("{","").replace("}","").replace("(","").replace(")","").replace("\n","").replace("\t","").replace("=","").replace(".",""))
  return df

# COMMAND ----------

# DBTITLE 1,Trigger to align the columns 
df1=get_column_renamed(df)

# COMMAND ----------

# DBTITLE 1,Create as delta table for aligned columns  
df1.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("MOCA.RAW_DATA_MOC")

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.removeAll()
dbutils.widgets.text('SecretScope','')
dbutils.widgets.text("JdbcUrlKeyDW",'')
dbutils.widgets.text("BlobStorage",'')
dbutils.widgets.text("BlobAccessKey",'')

# COMMAND ----------

# DBTITLE 1,Configuration to write in DW

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

# DBTITLE 1,Function which writes to DW
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

# DBTITLE 1,triggering the function which writes to DW
ExecuteSQLDWLoadMain("RAW_DATA_MOC","Y")