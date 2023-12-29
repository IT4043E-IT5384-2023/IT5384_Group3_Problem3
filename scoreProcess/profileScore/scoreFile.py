import csv
import datetime
import re
# Tạo danh sách các trường với kiểu dữ liệu int
fields = [
    "rest_id"
    "followerCount",
    "friend_followerRatio",
    "tweetFrequency",
    "tweet_followerRatio",
    "numOfFriends",
    "dateOfLastTweet",
    "verifiedAccount",
    "numOfLists",
    "favoritedCount",
    "accountAge",
    "bioCompleteness",
    "profilePicture",
    "protectedAccount",
    "totalScore"
]

# Mở file CSV để ghi
with open("score_data.csv", "w", newline="") as csvfile:
   writer = csv.writer(csvfile)

   # Ghi hàng tiêu đề
   writer.writerow(fields)

   # Thêm dữ liệu mẫu (bạn có thể thay thế bằng dữ liệu thực tế)
   writer.writerow([
       '',
       -1,  # followerCount
       -1,  # friend_followerRatio
       -1,  # tweetFrequency
       -1,  # tweet_followerRatio
       -1,  # numOfFriends
       -1,  # dateOfLastTweet
       -1,  # verifiedAccount
       -1,  # numOfLists
       -1,  # favoritedCount
       -1,  # accountAge
       -1,  # bioCompleteness
       -1,  # profilePicture
       -1,  # protectedAccount
       -1,
    ])


#
# def calculate_scores(data_file, score_file):
#    """
#    Đọc các trường từ file data và tính toán cho các trường của file score
#    """
#
#    with open(data_file, "r") as csvfile:
#        reader = csv.DictReader(csvfile)
#        scores = []
#        for row in reader:
#            score = {
#                "rest_id": row["rest_id"],
#                "followerCount": 10 if int(row["followers_count"]) >= 3000 else 5 if int(row["followers_count"]) >= 400 else 2 if int(row["followers_count"]) >= 50 else 0,
#                "friend_followerRatio": 15 if float(row["friends_count"]) / float(row["followers_count"]) > 5 else 10 if float(row["friends_count"]) / float(row["followers_count"]) > 2 else 5 if float(row["friends_count"]) / float(row["followers_count"]) > 1 else 0,
#                "tweetFrequency": -1,  # Chưa có logic tính tweetFrequency
#                "tweet_followerRatio": 10 if float(row["statuses_count"]) / float(row["followers_count"]) < 100 else 0,
#                "numOfFriends": 10 if int(row["friends_count"]) > 100 else 0,
#                "dateOfLastTweet": -1,  # Chưa có dữ liệu dateOfLastTweet
#                "verifiedAccount": 5 if row["verified"] == "true" else 0,
#                "numOfLists": 5 if int(row["listed_count"]) > 5 else 0,
#                "favoritesCount": 11 if int(row["favourites_count"]) > 2000 else 5,
#                "accountAge": 5 if (datetime.date.today() - datetime.datetime.strptime(row["join_time"], "%Y-%m-%d").date()).days > 365 else 0,
#                "bioCompleteness": 4 if row["description"] and row["location"] else 0,
#                "profilePicture": 3 if row["default_profile_image"] != "null" else 0,
#                "protectedAccount": 4 if row["private"] == "false" else 0,
#                "totalScore": sum([value for key, value in score.items() if key != "rest_id"])
#            }
#            scores.append(score)
#
#    with open(score_file, "w", newline="") as csvfile:
#        fieldnames = ["rest_id", "followerCount", "friend_followerRatio", "tweetFrequency", "tweet_followerRatio", "numOfFriends", "dateOfLastTweet", "verifiedAccount", "numOfLists", "favoritesCount", "accountAge", "bioCompleteness", "profilePicture", "protectedAccount"]
#        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#        writer.writeheader()
#        writer.writerows(scores)


def calculate_scores(data_file, tweet_file, score_file):
    """
    Đọc các trường từ file data và tweet, tính toán cho các trường của file score
    """

    with open(data_file, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        scores = []
        for row in reader:
            score = {
                "rest_id": row["rest_id"],
                "followerCount": 10 if int(row["followers_count"]) >= 3000 else 5 if int(row["followers_count"]) >= 400 else 2 if int(row["followers_count"]) >= 50 else 0,
                "friend_followerRatio": 0 if int(row["followers_count"]) == 0 else 15 if float(row["friends_count"]) / float(row["followers_count"]) > 5 else 10 if float(row["friends_count"]) / float(row["followers_count"]) > 2 else 5 if float(row["friends_count"]) / float(row["followers_count"]) > 1 else 0,
                "tweetFrequency": 0,  # Chưa có logic tính tweetFrequency
                "tweet_followerRatio": 0 if int(row["followers_count"]) == 0 else 10 if (float(row["statuses_count"]) / float(row["followers_count"])) < 50 else 5 if (float(row["statuses_count"]) / float(row["followers_count"])) < 100 else 0,
                "numOfFriends": 10 if int(row["friends_count"]) > 100 else 5 if int(row["friends_count"]) > 50 else 0,
                "dateOfLastTweet": 0,  # Chưa có dữ liệu dateOfLastTweet
                "verifiedAccount": 5 if row["verified"] == "true" else 0,
                "numOfLists": 5 if int(row["listed_count"]) > 5 else 3 if int(row["listed_count"]) > 2 else 0,
                "favoritesCount": 11 if int(row["favourites_count"]) > 2000 else 5 if int(row["favourites_count"]) > 1000 else 0,
                "accountAge": 5 if (datetime.date(2023, 11, 15) - datetime.datetime.strptime(row["join_time"], "%a %b %d %H:%M:%S %z %Y").date()).days > 365 else 3 if (datetime.date(2023, 11, 15) - datetime.datetime.strptime(row["join_time"], "%a %b %d %H:%M:%S %z %Y").date()).days > 90 else 0,
                "bioCompleteness": 4 if row["description"] and row["location"] else 0,
                "profilePicture": 3 if row["default_profile_image"] != "null" else 0,
                "protectedAccount": 4 if row["private"] == "false" else 0,
                # "totalScore": sum([value for key, value in score.items() if key != "rest_id"])
            }


            # Tính tweetFrequency
            tweets = []

            with open(tweet_file, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for tweet in reader:
                    if tweet["user_id"] == row["rest_id"]:
                        tweets.append(tweet)

            if len(tweets) > 0:
                total_seconds = []
                time_diffs = [tweet["time_diff"] for tweet in tweets]
                for time in time_diffs:
                    time = str(time)
                    if time == "":
                        time = "0 days 00:00:00"
                    days_match = re.search(r"(.*?) days", time)
                    days = days_match.group(1)
                    time_part = time[days_match.end():]

                    # Split time part
                    hours, minutes, seconds = time_part.split(":")
                    seconds = int(days) * 86400 + int(hours) * 3600 + int(minutes) * 60 + int(seconds)
                    total_seconds.append(seconds)
                if len(time_diffs) > 0:
                    average_time_diff = sum(total_seconds) / len(time_diffs)
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
                # Tìm tweet có date gần đây nhất
                date_of_last_tweet = min([datetime.datetime.strptime(tweet["date"], "%Y-%m-%d").date() for tweet in tweets])

                # Tính khoảng cách từ date gần nhất đến ngày 15/11/2023
                days_diff = (datetime.date(2023, 11, 15) - date_of_last_tweet).days

                if days_diff < 2:
                    score["dateOfLastTweet"] = 8
                elif days_diff < 7:
                    score["dateOfLastTweet"] = 4
                else:
                    score["dateOfLastTweet"] = 0

            score["totalScore"] = sum([value for key, value in score.items() if key != "rest_id"])
            scores.append(score)

    with open(score_file, "w", newline="") as csvfile:
        fieldnames = ["rest_id", "followerCount", "friend_followerRatio", "tweetFrequency", "tweet_followerRatio", "numOfFriends", "dateOfLastTweet", "verifiedAccount", "numOfLists", "favoritesCount", "accountAge", "bioCompleteness", "profilePicture", "protectedAccount", "totalScore"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(scores)


# Thay thế tên file data và score thực tế
data_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/accountDetails/cleanProfileData.csv"
tweet_file = "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/tweets/output_cleans.csv"
score_file = "score_data.csv"
calculate_scores(data_file, tweet_file, score_file)
