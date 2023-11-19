import json

# Đọc file JSON
with open('Accounts_details/realAcc2.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Lấy tên username từ dữ liệu
usernames = []
for record in data:
    for username, user_data in record.items():
        usernames.append(username)

# Lưu tên username vào file mới
with open('Username2.json', 'w', encoding='utf-8') as output_file:
    json.dump(usernames, output_file, ensure_ascii=False, indent=4)

print("Đã lưu tên username vào file Username.json")