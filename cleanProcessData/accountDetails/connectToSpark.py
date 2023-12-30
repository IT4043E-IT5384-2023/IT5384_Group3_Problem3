import pyspark.sql as spark
from pyspark.sql.functions import concat_ws
from pyspark.sql import SparkSession

# Create Spark Session
spark = spark.SparkSession.builder.appName("ETLJob").getOrCreate()
# spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
#          .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
#          .config("spark.executor.memory", "2G")  #excutor excute only 2G
#          .config("spark.driver.memory","4G")
#          .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
#          .config("spark.python.worker.memory","1G") # each worker use 1G to excute
#          .config("spark.driver.maxResultSize","2G") #Maximum size of result is 3G
#          .config("spark.kryoserializer.buffer.max","1024M")
#          .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#          .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
#         # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/bucket_connector/lucky-wall-393304-2a6a3df38253.json")
#          .config('spark.debug.maxToStringFields', 100)
#          .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")
#
#          .getOrCreate())
spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-2a6a3df38253.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

# Read data from parquet file
df = spark.read.parquet("C:/Users/DELL/Downloads/employees/part-00000-9abf32a3-db43-42e1-9639-363ef11c0d1c-c000.snappy.parquet")
df.show(10)

# Transform data
# df = df.withColumn("fullname", spark.sql("concat(first_name, ' ', last_name)"))
df = df.withColumn("fullname", concat_ws(" ", df.first_name, df.second_name))
df = df.withColumn("seat_height", df.floor * 2.5)

# Save data to CSV file
df.write.mode("overwrite").csv("employees.csv")

df.show(10)

# Stop Spark Session
spark.stop()


from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#config the connector jar file
# spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
        #  .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
        #  .config("spark.executor.memory", "2G")  #excutor excute only 2G
        # .config("spark.driver.memory","4G")
        # .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
        # .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        # .config("spark.driver.maxResultSize","2G") #Maximum size of result is 3G
        # .config("spark.kryoserializer.buffer.max","1024M")
        # .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        # .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/bucket_connector/lucky-wall-393304-2a6a3df38253.json")
        # .config('spark.debug.maxToStringFields', 100)
        # .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")
        #
        #  .getOrCreate())
#config the credential to identify the google cloud hadoop file


## Connect to the file in Google Bucket with Spark

#path= f"gs://it4043e-it5384/it5384/IT5384_Group2_Problem1/Data1.json"
#df =spark.read.json(path, multiLine=True)
#df.show()

#spark.stop() # Ending spark job