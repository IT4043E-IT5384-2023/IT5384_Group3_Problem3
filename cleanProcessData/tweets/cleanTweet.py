import pandas as pd
import html
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import json
from pandas import json_normalize

pd.set_option('display.max_colwidth', None)
data = pd.read_json('C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/twitter_scraper_selenium/tweets/tweet1.json')
df_list = []

for user_id, user_data in data.items():
    for tweet in user_data["tweets"]:
        tweet_info = {
            "id_str": tweet["id_str"],
            "date": tweet["date"],
            "lang": tweet["lang"],
            "rawContent": tweet["rawContent"],
            "likeCount": tweet["likeCount"],
            "retweetCount": tweet["retweetCount"],
            "replyCount": tweet["replyCount"],
            "user": {
                "id_str": tweet["user"]["id_str"],
                "displayname": tweet["user"]["displayname"],
                "followersCount": tweet["user"]["followersCount"],
                "friendsCount": tweet["user"]["friendsCount"],
                # Add other user fields as needed
            }
        }
        tweet_info["user_id"] = user_id
        df_list.append(tweet_info)

df = pd.DataFrame(df_list)
# Clean newline characters and unescape HTML entities
df['rawContent'] = [html.unescape(tweet.replace("\n", " ")) for tweet in df['rawContent']]

# Remove mentions, special characters, hashtags, and URLs using regular expressions
df['rawContent'] = df['rawContent'].apply(lambda x: re.sub(r'(@[A-Za-z0-9_]+)|[^\w\s]|#|http\S+', '', x))

# Convert 'date' to datetime format
df['date'] = pd.to_datetime(df['date'])

# Sort DataFrame by 'user_id' and 'date'
df.sort_values(by=['user_id', 'date'], inplace=True)

# Calculate time difference between tweets for each user
df['time_diff'] = df.groupby('user_id')['date'].diff()

df = df[(df['rawContent'].str.len() >= 10)]

# Prepare stopwords and tokenize tweets
sw = stopwords.words('english')
sw.remove('not')  # Exclude 'not' from stopwords
for col in df.columns:
    if df[col].dtype == 'object':  # Process only object (string) columns
        df[col] = df[col].apply(lambda x: ' '.join([word for word in word_tokenize(str(x)) if word.lower() not in sw]))
# Save DataFrame to CSV
df.to_csv('output_cleans.csv', index=False)