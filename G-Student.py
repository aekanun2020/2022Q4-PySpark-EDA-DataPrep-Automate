

import pyspark
from pyspark.sql import SparkSession
from operator import add
sc = pyspark.SparkContext()


spark = SparkSession(sc)

server_name = "jdbc:sqlserver://34.125.84.134"
database_name = "inclassdb"
url = server_name + ";" + "databaseName=" + database_name + ";"

table_name = "spark_flightdetail"
username = "SA"
password = "Passw0rd123456" # Please specify password here


# In[2]:


read_result_df = spark.read.format("jdbc")        .option("url", url)         .option("dbtable", table_name)         .option("user", username)         .option("password", password)        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()


# In[3]:


#read_result_df.count()


# In[4]:


read_result_df.write.mode('overwrite').parquet('/rawzone/')

