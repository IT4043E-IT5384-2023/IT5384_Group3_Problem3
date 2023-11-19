import jsonlines
import ast
import json
import yaml

# def filter_json(input_file, output_file):
#     with open(input_file, 'r') as infile, jsonlines.open(output_file, 'w') as outfile:
#         # outfile = ast.literal_eval(jsonlines.dumps(outfile))
#         for data in infile:
#             # Kiểm tra nếu __typename là "UserUnavailable" thì bỏ qua
#             # if any(record.get("user", {}).get("result", {}).get("__typename") == "UserUnavailable" for record in data.values()):
#             #     continue
#             if any(record.get('user', {}).get('result', {}).get('__typename') == 'UserUnavailable' for record in data):
#                 continue
#
#             # Ghi lại các bản ghi khác vào file output
#             outfile.write(data)
#
#
# # Thay 'input.jsonl' và 'output.jsonl' bằng tên file thực tế của bạn
# filter_json('acc.json', 'realAcc.json')

# import ast
# import json
#
# inpt = {'http://example.org/about': {'http://purl.org/dc/terms/title':
#                                      [{'type': 'literal', 'value': "Anna's Homepage"}]}}
#
# json_data = ast.literal_eval(json.dumps(inpt))
#
# print(json_data)
#
#
# def filter_json(input_file, output_file):
#     with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
#         data = json.load(infile)
#         # data = yaml.safe_load(infile)
#         # Loại bỏ các bản ghi có trường __typename là "UserUnavailable"
#         # filtered_data = {
#         #     key: value for key, value in data.items() if  value.get("user", {}).get("result", {}).get("__typename") != "UserUnavailable"
#         # }
#         #
#         # # Ghi lại dữ liệu đã được lọc vào file output
#         # json.dump(filtered_data, outfile, ensure_ascii=False, indent=2)
#
#         for data in infile:
#
#             # if any(record.get("user", {}).get("result", {}).get("__typename") == "UserUnavailable" for record in data.values()):
#             #     continue
#             # Ghi lại các bản ghi khác vào file output
#             data1 = record['user'] for record in data.values()
#             outfile.write(data)
#
# # Thay 'input.json' và 'output.json' bằng tên file thực tế của bạn
# filter_json('acc.json', 'realAcc.json')

# import json
#
# # Đọc file JSONL
# with open('acc.json', 'r') as file:
#     # Lặp qua từng dòng trong file
#     for line in file:
#         # Chuyển đổi chuỗi JSONL thành đối tượng Python
#         data = json.loads(line)
#
#         # Truy cập giá trị của trường "__typename" từ mỗi bản ghi
#         typename_value = data.get('user', {}).get('result', {}).get('__typename')
#
#         # In giá trị "__typename" của mỗi bản ghi
#         print(typename_value)

# import json
#
# # Đọc file JSONL
# with open('acc.json', 'r') as file:
#     # Lặp qua từng dòng trong file
#     for line in file:
#         # Chuyển đổi chuỗi JSONL thành đối tượng Python
#         data = json.loads(line.strip())
#
#         # Lặp qua mỗi bản ghi trong dữ liệu
#         for username, user_data in data.items():
#             # Truy cập giá trị của trường "__typename"
#             typename_value = user_data.get('user', {}).get('result', {}).get('__typename')
#
#             # In giá trị "__typename" của mỗi bản ghi
#             print(f"Username: {username}, __typename: {typename_value}")

import json

# Đọc file JSONL
with open('twitter_api_data_fix.json', 'r') as file:
    # Khởi tạo danh sách để lưu trữ các bản ghi hợp lệ
    valid_records = []

    # Lặp qua từng dòng trong file
    for line in file:
        # Chuyển đổi chuỗi JSONL thành đối tượng Python
        data = json.loads(line.strip())

        # Lặp qua mỗi bản ghi trong dữ liệu
        for username, user_data in data.items():
            # Truy cập giá trị của trường "__typename"
            typename_value = user_data.get('user', {}).get('result', {}).get('__typename')

            # Kiểm tra nếu __typename không phải là "UserUnavailable"
            if typename_value != "UserUnavailable":
                # Nếu là một bản ghi hợp lệ, thêm vào danh sách
                valid_records.append({username: user_data})

# Lưu danh sách các bản ghi hợp lệ vào file JSON mới
with open('realAcc2.json', 'w') as output_file:
    json.dump(valid_records, output_file, indent=4)

print("Các bản ghi hợp lệ đã được lưu vào realAcc2.json")

