import json

try:
    with open('twitter_api_data.json', 'r') as file:
        data = json.load(file)
    print("File JSON hợp lệ.")
except json.JSONDecodeError as e:
    print(f"Lỗi khi giải mã JSON: {e}")

# import json
#
# import os
# import json
#
#
# with open("twitter_api_data.jsonl", "r") as file:
#     raw_data = file.readlines()
#     raw_data = [line.strip() for line in raw_data]
#
# json_objects = []
# first = 0
# for i, content in enumerate(raw_data):
#     if i < len(raw_data) - 1:
#         if content == "}" and raw_data[i + 1] == "{":
#             json_objects.append("".join(raw_data[first:i+1]))
#             first = i + 1
#             continue
#     else:
#         assert content == "}", content
#         json_objects.append("".join(raw_data[first:]))
#
# with open("twitter_api_data_fix.json", "w") as writer:
#     writer.write("".join(json_objects))

import os
import json


# with open("twitter_api_data_account2_details.jsonl", "r") as file:
#     raw_data = file.readlines()
#     raw_data = [line.strip() for line in raw_data]
#
# json_objects = []
# first = 1
# for i, content in enumerate(raw_data):
#     if i < len(raw_data) - 1:
#         if content == "}" and raw_data[i + 1] == "{":
#             json_objects.append("".join(raw_data[first:i]))
#             first = i + 2
#             continue
#     else:
#         assert content == "}", content
#         json_objects.append("".join(raw_data[first:]))
#
# with open("twitter_api_data_fix.json", "w") as writer:
#     a = ",".join(json_objects)
#     # print(a[1408:1413])
#     writer.write("{" + a + "}\n")
