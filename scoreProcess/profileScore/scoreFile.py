import pandas as pd
import datetime
import re

def seconds_from_timedelta(time_delta):
    return time_delta.total_seconds() if not pd.isnull(time_delta) else 0
def calculate_scores(data_file, tweet_file, score_file):
    # Đọc dữ liệu từ tệp CSV bằng pandas
    data_df = pd.read_csv(data_file)
    tweet_df = pd.read_csv(tweet_file)
    tweet_df['time_diff'] = pd.to_timedelta(tweet_df['time_diff'])
    tweet_df['total_seconds'] = tweet_df['time_diff'].apply(seconds_from_timedelta)
    # Tạo danh sách để lưu trữ điểm số và thêm tiêu đề
    scores_list = [
        [
            "rest_id", "followerCount", "friend_followerRatio", "tweetFrequency",
            "tweet_followerRatio", "numOfFriends", "dateOfLastTweet",
            "verifiedAccount", "numOfLists", "favoritedCount", "accountAge",
            "bioCompleteness", "profilePicture", "protectedAccount", "totalScore"
        ]
    ]

    # Tạo một dictionary để lưu trữ tweets theo user_id
    tweets_by_user_id = tweet_df.groupby("user_id").apply(lambda group: group.to_dict(orient="records")).to_dict()

    for _, row in data_df.iterrows():
        score = {
            "rest_id": row["rest_id"],
            "followerCount": 10 if row["followers_count"] >= 3000 else 5 if row["followers_count"] >= 400 else 2 if row["followers_count"] >= 50 else 0,
             "friend_followerRatio": 0 if row["followers_count"] == 0 else 15 if row["friends_count"] / row["followers_count"] > 5 else 10 if row["friends_count"] / row["followers_count"] > 2 else 5 if row["friends_count"] / row["followers_count"] > 1 else 0,
            "tweetFrequency": 0,  # Chưa có logic tính tweetFrequency
            "tweet_followerRatio": 0 if row["followers_count"] == 0 else 10 if (row["statuses_count"] / row["followers_count"]) < 50 else 5 if (row["statuses_count"] / row["followers_count"]) < 100 else 0,
            "numOfFriends": 10 if row["friends_count"] > 100 else 5 if row["friends_count"] > 50 else 0,
            "dateOfLastTweet": 0,  # Chưa có dữ liệu dateOfLastTweet
            "verifiedAccount": 5 if row["verified"] == "true" else 0,
            "numOfLists": 5 if row["listed_count"] > 5 else 3 if row["listed_count"] > 2 else 0,
            "favoritedCount": 11 if row["favourites_count"] > 2000 else 5 if row["favourites_count"] > 1000 else 0,
            "accountAge": 5 if (datetime.date(2023, 11, 15) - pd.to_datetime(row["join_time"]).date()).days > 365 else 3 if (datetime.date(2023, 11, 15) - pd.to_datetime(row["join_time"]).date()).days > 90 else 0,
            "bioCompleteness": 4 if pd.notna(row["description"]) and pd.notna(row["location"]) else 0,
            "profilePicture": 3 if pd.notna(row["default_profile_image"]) and row["default_profile_image"] != "null" else 0,
            "protectedAccount": 4 if row["private"] == "false" else 0,
        }

        # Tính tweetFrequency
        tweets = tweets_by_user_id.get(row["rest_id"], [])
        if len(tweets) > 0:
            total_seconds = [tweet["total_seconds"] for tweet in tweets]
            if len(total_seconds) > 0:
                average_time_diff = sum(total_seconds) / len(total_seconds)
                if 86400 / 6 > average_time_diff > 86400 / 12:
                    score["tweetFrequency"] = 8
                elif average_time_diff > 86400 / 12:
                    score["tweetFrequency"] = 4
                else:
                    score["tweetFrequency"] = 0
            else:
                score["tweetFrequency"] = 0
        else:
            score["tweetFrequency"] = 0

        # Tính dateOfLastTweet
        if len(tweets) > 0:
            date_of_last_tweet = min([pd.to_datetime(tweet["date"]).date() for tweet in tweets])
            days_diff = (datetime.date(2023, 11, 15) - date_of_last_tweet).days

            if days_diff < 2:
                score["dateOfLastTweet"] = 8
            elif days_diff < 7:
                score["dateOfLastTweet"] = 4
            else:
                score["dateOfLastTweet"] = 0

        score["totalScore"] = sum(score.values()) - score["rest_id"]  # Trừ điểm của rest_id
        scores_list.append(list(score.values()))

    # Tạo DataFrame từ danh sách điểm số và lưu vào tệp CSV
    scores_df = pd.DataFrame(scores_list[1:], columns=scores_list[0])
    scores_df.to_csv(score_file, index=False)

data_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/accountDetails/cleanProfileData2.csv"
tweet_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/tweets/output_cleans0.csv"
score_file = "score_data.csv"
calculate_scores(data_file, tweet_file, score_file)
