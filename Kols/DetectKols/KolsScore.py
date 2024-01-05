
import pandas as pd
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


# Đọc dữ liệu từ file CSV
account_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/scoreProcess/profileScore/score_data.csv"
tweet_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/Models/results.csv"

df1 = spark.read.csv(account_file)
account_df = df1.toPandas()
df2 = spark.read.csv(tweet_file)
tweet_df = df1.toPandas()


# Group tweets by user_id
grouped_tweets = tweet_df.groupby('user_id')

# Duyệt qua từng nhóm và cập nhật thông tin vào DataFrame của account
for user_id, tweet_group in grouped_tweets:
    if user_id in account_df['rest_id'].values:
        # Tính toán tỉ lệ
        total_tweets = len(tweet_group)
        spam_percentage = (tweet_group['Spam'] == 'Spam').sum() / total_tweets * 100
        negative_sentiment_percentage = (tweet_group['sentiment'] == 'Negative').sum() / total_tweets * 100
        business_entrepreneurs_percentage = ((tweet_group['Topic'] != 'diaries_&_daily_life')
                                             & (tweet_group['Topic'] != 'news_&_social_concern')
                                             & (tweet_group['Topic'] != 'celebrity_&_pop_culture')
                                             & (tweet_group['Topic'] != 'other_hobbies')
                                             & (tweet_group['Topic'] != 'family')
                                             & (tweet_group['Topic'] != 'relationships')
                                             & (tweet_group['Topic'] != 'youth_&_student_life')
                                             & (tweet_group['Topic'] != 'business_&_entrepreneurs')).sum() / total_tweets * 100
        like_count_not_retweet_percentage = ((tweet_group['likeCount'] > 1000) & (
                    tweet_group['retweet'] == False)).sum() / total_tweets * 100

        # Gán giá trị cho các cột mới
        account_df.loc[account_df['rest_id'] == user_id, 'spam_percentage'] = spam_percentage
        account_df.loc[
            account_df['rest_id'] == user_id, 'negative_sentiment_percentage'] = negative_sentiment_percentage
        account_df.loc[
            account_df['rest_id'] == user_id, 'business_entrepreneurs_percentage'] = business_entrepreneurs_percentage
        account_df.loc[
            account_df['rest_id'] == user_id, 'like_count_not_retweet_percentage'] = like_count_not_retweet_percentage
    else:
        # Nếu user_id không tồn tại trong DataFrame tweet_df, đặt giá trị mặc định là -1
        account_df.loc[account_df['rest_id'] == user_id, 'spam_percentage'] = -1
        account_df.loc[account_df['rest_id'] == user_id, 'negative_sentiment_percentage'] = -1
        account_df.loc[account_df['rest_id'] == user_id, 'business_entrepreneurs_percentage'] = -1
        account_df.loc[account_df['rest_id'] == user_id, 'like_count_not_retweet_percentage'] = -1
# Ghi DataFrame của account xuống file CSV mới
account_df.to_csv("KolsScore.csv", index=False)

