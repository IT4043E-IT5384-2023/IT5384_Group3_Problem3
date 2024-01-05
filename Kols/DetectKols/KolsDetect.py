import pandas as pd
from datetime import datetime, timezone

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
    elif 35 < totalScore_acc <= 64:
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
account_df = pd.read_csv("C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Kols/DetectKols/KolsScore.csv")
clean_df = pd.read_csv("C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/accountDetails/ProfileData.csv")

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