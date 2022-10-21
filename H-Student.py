import pyspark
from pyspark.sql import SparkSession
from operator import add
sc = pyspark.SparkContext()

spark = SparkSession(sc)


# coding: utf-8

# In[1]:


from pyspark.sql import functions as sparkf


# In[2]:


#import time as t
#start_time = t.time()


# ### 1. Business Understanding

# Problem Statement:  Prediction of Arrival Delay

# Project Objective: (1) Regression Model (2) Model Deployment

# ### 2. Data Understanding

# #### Spark อ่านไฟล์ 2008.csv จาก HDFS มาเป็น DataFrame

# In[3]:


airline_df = spark.read.parquet('/rawzone/*')


# #### Spark นับจำนวน tuple ใน DataFrame

# In[4]:


#airline_df.count()


# #### Spark: Assign ค่าของตัวแปรเก่า ให้กับตัวแปรใหม่

# In[5]:


airline_row_df = airline_df.filter(sparkf.col('Cancelled') != 1)


# In[6]:


#airline_row_df.count()


# #### Spark แสดง Schema ของข้อมูลใน DataFrame

# In[7]:


#airline_row_df.printSchema()


# In[8]:


#airline_row_df.describe().toPandas().transpose()


# ### 3. Data Preparation

# #### Spark เรียกใช้ Data Types และ Functions ต่างๆ สำหรับจัดการข้อมูลใน DataFrame

# In[9]:


from pyspark.sql.types import *
from pyspark.sql.functions import col, udf


# In[10]:


crunched_df = airline_row_df.withColumn('DepTime',airline_row_df['DepTime'].           cast(DoubleType())).withColumn('TaxiOut',airline_row_df['TaxiOut'].           cast(DoubleType())).withColumn('TaxiIn',airline_row_df['TaxiIn'].           cast(DoubleType())).withColumn('DepDelay',airline_row_df['DepDelay'].           cast(DoubleType())).withColumn('DayOfWeek',airline_row_df['DayOfWeek'].           cast(DoubleType())).withColumn('Distance',airline_row_df['Distance'].           cast(DoubleType())).withColumn('ArrDelay',airline_row_df['ArrDelay'].           cast(DoubleType()))


# #### Spark แสดง Schema ของข้อมูลใน DataFrame หลังจาก cast type แล้ว

# In[11]:


#crunched_df.printSchema()


# #### Python ติดตั้ง Module "pandas"

# In[12]:


#get_ipython().system(' pip install pandas')


# #### Spark ทำ Data Exploratory โดยใช้สถิติเบื้องต้นกับข้อมูลใน DataFrame




# #### Spark ทำ Data Transformation โดยใช้ Data Discretization กับ "DepTime" ใน DataFrame

# In[14]:


def t_timeperiod(origin):
    if origin is None:
        period = None
    elif origin > 0 and origin < 600:
        period = '00.01-05.59'
    elif origin >= 600 and origin <=1200:
        period = '06.00-11.59'
    elif origin >= 1200 and origin <= 1800:
        period = '12.00-17.59'
    elif origin >= 1800 and origin <= 2400:
        period = '18.00-24.00'
    else:
        period = 'NA'
    return period


# In[15]:


timeperiod = udf(lambda x: t_timeperiod(x),StringType())


# In[16]:


discretized_df = crunched_df.withColumn('DepTime',timeperiod(crunched_df['DepTime']))


# #### Spark ทำ Data Transformation โดยใช้ Data Normalization กับ "Distance" และ "ArrDelay" ใน DataFrame

# In[17]:


from pyspark.sql.functions import *
max_distance = discretized_df.select(max('Distance')).collect()[0][0]
min_distance = discretized_df.select(min('Distance')).collect()[0][0]


# In[18]:


max_ArrDelay = discretized_df.select(max('ArrDelay')).collect()[0][0]
min_ArrDelay = discretized_df.select(min('ArrDelay')).collect()[0][0]


# In[19]:


def t_normalized_distance(origin):
    if origin is None:
        return None
    else:
        return ((origin-min_distance)/(max_distance-min_distance))


# In[20]:


def t_normalized_ArrDelay(origin):
    if origin is None:
        return None
    else:
        return ((origin-min_ArrDelay)/(max_ArrDelay-min_ArrDelay))


# In[21]:


normalized_distance = udf(lambda x: t_normalized_distance(x),DoubleType())


# In[22]:


normalized_ArrDelay = udf(lambda x: t_normalized_ArrDelay(x),DoubleType())


# In[23]:


normalized_df = discretized_df

normalized_df = discretized_df.\
withColumn('Distance', normalized_distance(discretized_df['Distance'])).\
withColumn('ArrDelay', normalized_ArrDelay(discretized_df['ArrDelay']))
# #### Spark ทำ Feature Selection ด้วยการเลือกเฉพาะบาง Attributes มาเป็น Features

# In[24]:


features_df = normalized_df.select(['UniqueCarrier','Origin','Dest',        'DepTime','TaxiOut','TaxiIn','DepDelay',        'DayOfWeek','Distance','ArrDelay'])


# #### Spark กำจัดค่า Null ด้วยการลบทั้ง Tuple (Record) เมื่อพบว่ามี Attribute ใดมีค่า Null

# In[25]:


final_df = features_df.dropna()


# #### Spark นับจำนวน tuple ใน DataFrame

# In[26]:


#final_df.count()


# In[27]:


#final_df.show()


# ### 4. Modeling (and making some data transformation )

# #### Spark แบ่งข้อมูลเป็น training set และ test set

# In[28]:


training_df,test_df = final_df.randomSplit([0.80,0.20], seed = 12)


# #### Spark นับจำนวน tuple ใน DataFrame

# In[29]:


#training_df.count()


# #### Spark แสดง Schema ของ training set

# In[30]:


#training_df.printSchema()


# #### Transformation categorical variable to numerical one.

# In[31]:


from pyspark.ml.feature import StringIndexer,OneHotEncoder


# In[32]:


DepTimeIndexer = StringIndexer(inputCol='DepTime',outputCol='DepTimeIndexed',handleInvalid='keep')


# In[33]:


UniqueCarrierIndexer = StringIndexer(inputCol='UniqueCarrier', outputCol='UniqueCarrierIndexed',handleInvalid='keep')


# In[34]:


UniqueCarrierOneHotEncoder = OneHotEncoder(dropLast=False,inputCol='UniqueCarrierIndexed', outputCol='UniqueCarrierVec')


# In[35]:


OriginIndexer = StringIndexer(inputCol='Origin',                              outputCol='OriginIndexed',handleInvalid='keep')


# In[36]:


OriginOneHotEncoder = OneHotEncoder(dropLast=False,inputCol='OriginIndexed', outputCol='OriginVec')


# In[37]:


DestIndexer = StringIndexer(inputCol='Dest',                            outputCol='DestIndexed',handleInvalid='keep')


# In[38]:


DestOneHotEncoder = OneHotEncoder(dropLast=False,inputCol='DestIndexed', outputCol='DestVec')


# In[39]:


#labelIndexer = StringIndexer(inputCol='ArrDelay',outputCol='labelIndexed')


# #### Combines a selected columns into a single vector column.

# In[40]:


from pyspark.mllib.linalg import Vectors


# In[41]:


from pyspark.ml.feature import VectorAssembler


# In[42]:


from pyspark.ml import Pipeline


# In[43]:


featureAssembler = VectorAssembler(inputCols=['UniqueCarrierIndexed',            'OriginVec',            'DepTimeIndexed',            'TaxiOut','TaxiIn',            'DepDelay',            'DayOfWeek',            'Distance'
           ], outputCol='***features')


# #### Define an algorithm.

# In[44]:


from pyspark.ml.regression import RandomForestRegressor, LinearRegression


# In[45]:


dt = LinearRegression(labelCol='ArrDelay',featuresCol='***features')


# #### Pipeline.

# In[46]:


pipeline_dt = Pipeline().setStages([UniqueCarrierIndexer,           UniqueCarrierOneHotEncoder,           DepTimeIndexer,           OriginIndexer ,           OriginOneHotEncoder,           DestIndexer,           DestOneHotEncoder,           featureAssembler,dt])


# In[47]:


#training_df.count()


# #### Launch the pipeline and get a model.

# In[48]:


dtModel = pipeline_dt.fit(training_df)


# #### print out model structure

# In[49]:


#tree = dtModel.stages[8]


# In[50]:


#tree.toDebugString






dtModel.write().overwrite().save('gs://oct22-23/refinedzone/ArrDelay_regressionModel')

