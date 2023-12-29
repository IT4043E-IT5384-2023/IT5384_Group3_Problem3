# from twitter_scraper_selenium.profile_details import get_profiles_detailslist
# import csv
# import time
#
# def get_records_by_field(csv_file, field_name):
#     records = []
#
#     with open(csv_file, 'r', newline='') as file:
#         csv_reader = csv.DictReader(file)
#
#         for row in csv_reader:
#             if field_name in row:
#                 records.append(row[field_name])
#
#     return records
#
#
# twitter_username = get_records_by_field('Username.csv','screenName')
# # filename = 'Username.csv'
# # twitter_username = {}
# # with open(filename, 'r') as file:
# #     file.seek(0)  # Di chuyển con trỏ về đầu file
# #     reader = csv.reader(file)
# #     # Lặp qua từng dòng
# #     for row in reader:
# #         # Lấy nội dung của dòng
# #         twitter_username = row
#
#
# output_file = "twitter_api_data"
#
#
# get_profiles_detailslist(twitter_usernames=twitter_username, filename=output_file)
#

import os
from twitter_scraper_selenium import scrape_profile


if not os.path.exists("./tweets"):
    os.mkdir("./tweets")
scrape_profile(twitter_username="elonmusk",output_format="csv",browser="chrome",tweets_count=10,filename="elonmusk",directory="./tweets")