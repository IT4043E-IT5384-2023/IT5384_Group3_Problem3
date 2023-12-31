# import pandas as pd
# # Đọc dữ liệu từ file CSV
# account_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/scoreProcess/profileScore/score_data.csv"
# model_result = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Models/model_result.csv"
# account_df = pd.read_csv(account_file)
# tweet_df = pd.read_csv(model_result)
#
# # Group tweets by user_id
# grouped_tweets = tweet_df.groupby('user_id')
#
# # Duyệt qua từng nhóm và cập nhật thông tin vào DataFrame của account
# for user_id, tweet_group in grouped_tweets:
#     if user_id in account_df['rest_id'].values:
#         # Tính toán tỉ lệ
#         total_tweets = len(tweet_group)
#         spam_percentage = (tweet_group['Spam'] == 'Spam').sum() / total_tweets * 100
#         negative_sentiment_percentage = (tweet_group['sentiment'] == 'Negative').sum() / total_tweets * 100
#         non_diaries_daily_life_percentage = (tweet_group['Topic'] != 'diaries_&_daily_life').sum() / total_tweets * 100
#         like_count_not_retweet_percentage = ((tweet_group['likeCount'] > 1000) & (tweet_group['retweet'] is False)).sum() / total_tweets * 100
#
#         # Gán giá trị cho các cột mới
#         account_df.loc[account_df['rest_id'] == user_id, 'spam_percentage'] = spam_percentage
#         account_df.loc[account_df['rest_id'] == user_id, 'negative_sentiment_percentage'] = negative_sentiment_percentage
#         account_df.loc[account_df['rest_id'] == user_id, 'non_diaries_daily_life_percentage'] = non_diaries_daily_life_percentage
#         account_df.loc[account_df['rest_id'] == user_id, 'like_count_not_retweet_percentage'] = like_count_not_retweet_percentage
#
# # Ghi DataFrame của account xuống file CSV mới
# account_df.to_csv("KolsScore.csv", index=False)

import pandas as pd

# Đọc dữ liệu từ file CSV
account_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/scoreProcess/profileScore/score_data.csv"
tweet_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Models/results.csv"
account_df = pd.read_csv(account_file)
tweet_df = pd.read_csv(tweet_file)

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

