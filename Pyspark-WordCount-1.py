import pyspark
from pyspark.sql import SparkSession
from operator import add
sc = pyspark.SparkContext()

data_rdd = sc.parallelize(list("Hello World"))
keyval_rdd = data_rdd.map(lambda x:
    (x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
     ascending=False)

spark = SparkSession(sc)
hasattr(keyval_rdd, "toDF")

keyval_rdd.toDF(['Alphabet','Count']).write.mode('overwrite').csv('gs://19oct/wordCountResult/')
