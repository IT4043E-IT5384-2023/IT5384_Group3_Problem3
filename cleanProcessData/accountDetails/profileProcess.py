import json
import csv

# Thay đổi đường dẫn đến tệp JSON của bạn
json_file_path = 'C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/twitter_scraper_selenium/Accounts_details/realAcc2.json'

with open(json_file_path, 'r') as json_file:
    data = json.load(json_file)

# Mở tệp CSV để ghi dữ liệu
csv_file_path = 'cleanProfileData2.csv'
with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
    fieldnames = [
        'rest_id',
        'screen_name',
        'is_blue_verified',
        'description',
        'location',
        'url',
        'join_time',
        'default_profile_image',
        'statuses_count',
        'friends_count',
        'followers_count',
        'favourites_count',
        'listed_count',
        'media_count',
        'possibly_sensitive',
        'private',
        'verified',
        'profile_image_url_https',
        'profile_banner_url',
        'score',
        'label'
    ]

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()

    for user_data in data:
        user_data = user_data.get(list(user_data.keys())[0])  # Trích xuất giá trị từ dict
        legacy_data = user_data['user']['result']['legacy']

        row = {
            'rest_id': str(user_data['user']['result'].get('rest_id', '')),
            'screen_name': legacy_data.get('screen_name'),
            'is_blue_verified': user_data['user']['result'].get('is_blue_verified', False),
            'description': legacy_data.get('description', ''),
            'location': legacy_data.get('location', ''),
            'url': legacy_data['entities']['description']['urls'][0]['url'] if legacy_data.get('entities', {}).get('description', {}).get('urls') else None,
            'join_time': legacy_data.get('created_at', ''),
            'default_profile_image': legacy_data.get('default_profile_image', False),
            'statuses_count': legacy_data.get('statuses_count', 0),
            'friends_count': legacy_data.get('friends_count', 0),
            'followers_count': legacy_data.get('followers_count', 0),
            'favourites_count': legacy_data.get('favourites_count', 0),
            'listed_count': legacy_data.get('listed_count', 0),
            'media_count': legacy_data.get('media_count', 0),
            'possibly_sensitive': legacy_data.get('possibly_sensitive', False),
            'private': legacy_data.get('protected', False),
            'verified': legacy_data.get('verified', False),
            'profile_image_url_https': legacy_data.get('profile_image_url_https', ''),
            'profile_banner_url': legacy_data.get('profile_banner_url', ''),
            'score': -1,
            'label': ''
        }

        writer.writerow(row)

print(f'Data has been written to {csv_file_path}')

