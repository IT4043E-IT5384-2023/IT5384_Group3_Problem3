# from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#
# # Khởi tạo Spark Session
# # spark = (SparkSession.builder.appName("ConvertJSONtoCSV").master("local[*]")
# spark = (SparkSession.builder.appName("SimpleSparkJob").master("http://34.142.194.212:8000/")
#          .config("spark.executor.memory", "2G")  #excutor excute only 2G
#         .config("spark.driver.memory","4G")
#         .config("spark.executor.cores","1") #Cluster use only 3 cores to excute
#         .config("spark.python.worker.memory","1G") # each worker use 1G to excute
#         .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
#         .config("spark.kryoserializer.buffer.max","1024M")
#          .getOrCreate()) #http://34.142.194.212:4040


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "2G")  #excutor excute only 2G
        .config("spark.driver.memory","4G")
        .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
        .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        .config("spark.driver.maxResultSize","2G") #Maximum size of result is 3G
        .config("spark.kryoserializer.buffer.max","1024M")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        # .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/bucket_connector/lucky-wall-393304-2a6a3df38253.json")
        .config('spark.debug.maxToStringFields', 100)
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")

         .getOrCreate())
#config the credential to identify the google cloud hadoop file
# spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-2a6a3df38253.json")
# spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
# spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

## Connect to the file in Google Bucket with Spark

#path= f"gs://it4043e-it5384/it5384/IT5384_Group2_Problem1/Data1.json"
#df =spark.read.json(path, multiLine=True)
#df.show()

#spark.stop() # Ending spark job

# Đọc dữ liệu JSON thành DataFrame
# df = spark.read.json('C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/twitter_scraper_selenium/Accounts_details/realAcc.json')
df = spark.read.json('C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/twitter_scraper_selenium/Accounts_details/realAcc.json', multiLine=True)


# Chọn các cột cần thiết
df_selected = df.select(
    col("user.result.rest_id").alias("rest_id"),
    col("user.result.legacy.screen_name").alias("screen_name"),
    col("user.result.is_blue_verified").alias("is_blue_verified"),
    col("user.result.legacy.description").alias("description"),
    col("user.result.legacy.location").alias("location"),
    col("user.result.legacy.entities.description.urls.url").alias("url"),
    col("user.result.legacy.created_at").alias("join_time"),
    col("user.result.legacy.default_profile_image").alias("default_profile_image"),
    col("user.result.legacy.statuses_count").alias("statuses_count"),
    col("user.result.legacy.friends_count").alias("friends_count"),
    col("user.result.legacy.followers_count").alias("followers_count"),
    col("user.result.legacy.favourites_count").alias("favourites_count"),
    col("user.result.legacy.listed_count").alias("listed_count"),
    col("user.result.legacy.media_count").alias("media_count"),
    col("user.result.legacy.possibly_sensitive").alias("possibly_sensitive"),
    col("user.result.legacy.protected").alias("private"),
    col("user.result.legacy.verified").alias("verified"),
    col("user.result.legacy.profile_image_url_https").alias("profile_image_url_https"),
    col("user.result.legacy.profile_banner_url").alias("profile_banner_url"),
    col("").alias("label")  # Thêm cột label với giá trị rỗng
)

# Ghi DataFrame ra CSV
csv_file_path = 'cleanProfileData1.csv'
df_selected.write.csv(csv_file_path, header=True, mode="overwrite")

print(f'Data has been written to {csv_file_path}')

# Dừng SparkSession
spark.stop()
