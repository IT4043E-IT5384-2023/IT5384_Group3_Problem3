import pandas as pd
from datetime import datetime, timezone
def is_kol(account_data):
    spam_tweet_ratio_threshold = 20
    negative_sentiment_threshold = 20
    business_entrepreneurs_threshold = 40
    like_count_not_retweet_threshold = 50
    totalScore_acc_threshold = 60
    total_score_threshold = 8

    total_score = 0

    spam_tweet_ratio = account_data["spam_percentage"]  # Thêm trường này vào account_data
    negative_sentiment = account_data["negative_sentiment_percentage"]
    business_entrepreneurs_percentage = account_data["business_entrepreneurs_percentage"]
    like_count_not_retweet_percentage = account_data["like_count_not_retweet_percentage"]
    totalScore_acc = account_data["totalScore"]
    # Apply thresholds and calculate total score
    total_score += 1 if spam_tweet_ratio <= spam_tweet_ratio_threshold else 0
    total_score += 2 if negative_sentiment <= negative_sentiment_threshold else 0
    total_score += 2 if business_entrepreneurs_percentage >= business_entrepreneurs_threshold else 0
    total_score += 2 if like_count_not_retweet_percentage >= like_count_not_retweet_threshold else 0
    total_score += 3 if totalScore_acc >= totalScore_acc_threshold else 0
    return total_score >= total_score_threshold


account_df = pd.read_csv("C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Kols/DetectKols/KolsScore.csv")

# Thêm cột "IsKOL" vào DataFrame sử dụng hàm is_kol
account_df["IsKOL"] = account_df.apply(is_kol, axis=1)

account_df.to_csv("Detection KOL.csv", index=False)
