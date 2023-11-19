from twitter_scraper_selenium.profile_details import get_profiles_detailslist
import csv
import time

def get_records_by_field(csv_file, field_name):
    records = []

    with open(csv_file, 'r', newline='') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            if field_name in row:
                records.append(row[field_name])

    return records


twitter_username = get_records_by_field('Username.csv','screenName')


output_file = "twitter_api_data"


get_profiles_detailslist(twitter_usernames=twitter_username, filename=output_file)

