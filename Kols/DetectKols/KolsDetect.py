import pandas as pd
from datetime import datetime, timezone

from pyspark.sql import SparkSession

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
        .config('spark.debug.maxToStringFields', 100)
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")

         .getOrCreate())



def DetectLabel(account_data):
    follower_count_threshold = 8
    favorite_count_threshold = 9
    spam_tweet_ratio_threshold = 20
    negative_sentiment_threshold = 20
    business_entrepreneurs_threshold = 40
    like_count_not_retweet_threshold = 50
    totalScore_acc_threshold = 60
    total_score_threshold = 9
    total_score = 0

    follower_count = account_data["followerCount"]
    favorite_count = account_data["favoritedCount"]
    spam_tweet_ratio = account_data["spam_percentage"]  # Thêm trường này vào account_data
    negative_sentiment = account_data["negative_sentiment_percentage"]
    business_entrepreneurs_percentage = account_data["business_entrepreneurs_percentage"]
    like_count_not_retweet_percentage = account_data["like_count_not_retweet_percentage"]
    totalScore_acc = account_data["totalScore"]
    label = ""
    if totalScore_acc >= 65:
        label = "High quality"
    elif 40 < totalScore_acc <= 64:
        label = "Medium quality"
    else:
        label = "Low quality"
    # Apply thresholds and calculate total score
    total_score += 3 if follower_count >= follower_count_threshold else 0
    total_score += 1 if favorite_count >= favorite_count_threshold else 0
    total_score += 1 if spam_tweet_ratio <= spam_tweet_ratio_threshold else 0
    total_score += 2 if negative_sentiment <= negative_sentiment_threshold else 0
    total_score += 3 if business_entrepreneurs_percentage >= business_entrepreneurs_threshold else 0
    total_score += 2 if like_count_not_retweet_percentage >= like_count_not_retweet_threshold else 0
    total_score += 3 if totalScore_acc >= totalScore_acc_threshold else 0
    if total_score >= total_score_threshold:
        return "KOL"
    else:
        return label

# Read CSV files
# Đọc dữ liệu từ file CSV
account_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/Kols/DetectKols/KolsScore.csv"
profile_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/clean%2C%20processed%20data/Account%20profiles/ProfileData.csv"

df1 = spark.read.csv(account_file)
account_df = df1.toPandas()
df2 = spark.read.csv(profile_file)
clean_df = df1.toPandas()


# Add 'label' column to account_df
account_df["label"] = account_df.apply(DetectLabel, axis=1)

if 'label' in clean_df.columns:
    clean_df = clean_df.drop(columns=['label'])

label_mapping = dict(zip(account_df['rest_id'], account_df['label']))
# Ánh xạ giá trị 'label' vào 'clean_df' dựa trên cột 'rest_id'
clean_df['label'] = clean_df['rest_id'].map(label_mapping)

# Save the updated clean_df to a new CSV file
clean_df.to_csv("UpdatedCleanData.csv", index=False)

#clean_df.to_csv("C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/accountDetails/ProfileData.csv", index=False)