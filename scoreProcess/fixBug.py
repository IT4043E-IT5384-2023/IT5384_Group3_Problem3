import pandas as pd
#
# # Mở các file CSV
# files = ["C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Models/results_v1.csv", "C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Models/results_v22.csv"]
# dfs = []
# for file in files:
#     dfs.append(pd.read_csv(file))
#
# # Gộp các DataFrame
# df_merged = pd.concat(dfs)
#
# # Bỏ header của các file ghép vào sau
# if len(dfs) > 1:
#     df_merged = df_merged.iloc[1:]
#
# # Đặt lại chỉ số
# df_merged = df_merged.reset_index(drop=True)
#
# # Xuất DataFrame đã gộp
# df_merged.to_csv("C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Models/results.csv")
#
#
#
# # Đọc dữ liệu từ file CSV A
# df_a = pd.read_csv('C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/Kols/DetectKols/KolsScore.csv')
#
# # Đọc dữ liệu từ file CSV B
# df_b = pd.read_csv('C:/Users/DELL/PycharmProjects/IT5384_Group3_Problem3/cleanProcessData/accountDetails/cleanProfileData.csv')
#
# # Duyệt qua từng hàng của cả hai file CSV
# for row_a, row_b in zip(df_a.iterrows(), df_b.iterrows()):
#
#     # Lấy giá trị của cột A trong file CSV A
#     a = row_a[0]['IsKOL']
#
#     # Lấy giá trị của cột B trong file CSV B
#     b = row_b[0]['label']
#
#     # Nếu giá trị của cột A là True thì gán giá trị "Kols" cho cột B
#     if a:
#         b = 'True'
#
#     # Cập nhật giá trị của cột B
#     row_b[0]['Kols'] = b
#
# # # Ghi dữ liệu xuống file CSV B mới
# # df_b.to_csv('output.csv')



