import pandas as pd
import datetime
import re

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
        .config('spark.debug.maxToStringFields', 100)
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")

         .getOrCreate())




def seconds_from_timedelta(time_delta):
    return time_delta.total_seconds() if not pd.isnull(time_delta) else 0
def calculate_scores(data_file, tweet_file, score_file):

    # Đọc dữ liệu từ tệp CSV
    df1 = spark.read.csv(data_file)
    data_df = df1.toPandas()
    df2 = spark.read.csv(tweet_file)
    tweet_df = df2.toPandas()
    tweet_df['time_diff'] = pd.to_timedelta(tweet_df['time_diff'])
    tweet_df['total_seconds'] = tweet_df['time_diff'].apply(seconds_from_timedelta)
    # Tạo danh sách để lưu trữ điểm số và thêm tiêu đề
    scores_list = [
        [
            "rest_id", "followerCount", "friend_followerRatio", "tweetFrequency",
            "tweet_followerRatio", "numOfFriends", "dateOfLastTweet",
            "blueVerifiedAccount", "numOfLists", "favoritedCount", "accountAge",
            "bioCompleteness", "profilePicture", "protectedAccount", "totalScore"
        ]
    ]

    # Tạo một dictionary để lưu trữ tweets theo user_id
    tweets_by_user_id = tweet_df.groupby("user_id").apply(lambda group: group.to_dict(orient="records")).to_dict()

    for _, row in data_df.iterrows():
        score = {
            "rest_id": row["rest_id"],
            "followerCount": 10 if row["followers_count"] >= 20000 else 8 if row["followers_count"] >= 8000 else 5 if row["followers_count"] >= 1000 else 3 if row["followers_count"] >= 30 else 1,
            "friend_followerRatio": 0 if row["friends_count"] == 0 else 15 if row["followers_count"] / row["friends_count"] > 7 else 10 if row["followers_count"] / row["friends_count"] > 2 else 5 if row["followers_count"] / row["friends_count"] > 0 else 1,
            "tweetFrequency": 0,  # Chưa có logic tính tweetFrequency
            "tweet_followerRatio": 0 if row["followers_count"] == 0 else 10 if (row["statuses_count"] / row["followers_count"]) < 3 else 8 if (row["statuses_count"] / row["followers_count"]) < 5 else 5 if (row["statuses_count"] / row["followers_count"]) < 10 else 3 if (row["statuses_count"] / row["followers_count"]) < 20 else 1,
            "numOfFriends": 10 if row["friends_count"] > 5000 else 8 if row["friends_count"] > 2000 else 6 if row["friends_count"] > 100 else 4 if row["friends_count"] > 30 else 2,
            "dateOfLastTweet": 0,  # Chưa có dữ liệu dateOfLastTweet
            "blueVerifiedAccount": 5 if row["is_blue_verified"] is True else 0,
            "numOfLists": 5 if row["listed_count"] > 40 else 3 if row["listed_count"] > 5 else 1,
            "favoritedCount": 11 if row["favourites_count"] > 30000 else 9 if row["favourites_count"] > 10000 else 7 if row["favourites_count"] > 1000 else 5 if row["favourites_count"] > 100 else 3 if row["favourites_count"] > 30 else 1,
            "accountAge": 5 if (datetime.date(2023, 11, 17) - pd.to_datetime(row["join_time"]).date()).days > 1000 else 3 if (datetime.date(2023, 11, 17) - pd.to_datetime(row["join_time"]).date()).days > 500 else 1,
            "bioCompleteness": 4 if pd.notna(row["description"]) and pd.notna(row["location"]) else 1,
            "profilePicture": 3 if row["default_profile_image"] is False else 0,
            "protectedAccount": 4 if row["private"] is False else 0,
        }
        # print(score["blueVerifiedAccount"])
        # Tính tweetFrequency
        # print(tweets_by_user_id.get(row["rest_id"]))
        tweets = tweets_by_user_id.get(row["rest_id"], [])
        # print(tweets)
        if len(tweets) > 0:
            total_seconds = [tweet["total_seconds"] for tweet in tweets]
            if len(total_seconds) > 0:
                average_time_diff = sum(total_seconds) / len(total_seconds)
                # print(average_time_diff)
                if ((86400) >= average_time_diff) and (average_time_diff >= (86400 / 24)): # nhỏ hơn 1 ngày lớn hơn 1 tiếng
                    score["tweetFrequency"] = 8
                elif average_time_diff > (86400*10): #lớn hơn 10 ngày
                    score["tweetFrequency"] = 4
                elif average_time_diff > (86400) and average_time_diff <= (86400*10): # lớn hơn 1 ngày nhỏ hơn 10 ngày
                    score["tweetFrequency"] = 6
                else:
                    score["tweetFrequency"] = 2
                # print(score["tweetFrequency"])
            else:
                score["tweetFrequency"] = 2
        else:
            score["tweetFrequency"] = 2

        # Tính dateOfLastTweet
        if len(tweets) > 0:
            date_of_last_tweet = max([pd.to_datetime(tweet["date"]).date() for tweet in tweets])
            # print(date_of_last_tweet)
            days_diff = (datetime.date(2023, 11, 17) - date_of_last_tweet).days
            # print(days_diff)

            if days_diff < 2:
                score["dateOfLastTweet"] = 8
            elif days_diff < 7:
                score["dateOfLastTweet"] = 6
            elif days_diff < 30:
                score["dateOfLastTweet"] = 4
            else:
                score["dateOfLastTweet"] = 1

        score["totalScore"] = sum(score.values()) - score["rest_id"]  # Trừ điểm của rest_id
        print(score["totalScore"])
        # print(score["dateOfLastTweet"])
        scores_list.append(list(score.values()))

    # Tạo DataFrame từ danh sách điểm số và lưu vào tệp CSV
    scores_df = pd.DataFrame(scores_list[1:], columns=scores_list[0])
    scores_df.to_csv(score_file, index=False)

data_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/clean%2C%20processed%20data/Account%20profiles/ProfileData.csv"
tweet_file = f"gs://it4043e-it5384/it5384/IT5384_Group3_Problem3/clean%2C%20processed%20data/Tweets/tweetData.csv"
score_file = "score_data.csv"
calculate_scores(data_file, tweet_file, score_file)
