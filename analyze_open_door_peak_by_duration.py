import csv
from collections import defaultdict
from datetime import datetime, timedelta
import os

folder_path = ".\\logs"
data = dict()

recognize_time_files = dict()
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if "_recognize_time.csv" in file:
            ws = file.split('_')[0]
            recognize_time_files[ws] = os.path.join(root, file)

for ws in recognize_time_files.keys():
    _file = recognize_time_files[ws]
    with open(_file, 'r') as recognize_time_file:
        recognize_time_reader = csv.reader(recognize_time_file)
        for row in recognize_time_reader:
            if row[0] == 'recordId':
                continue
            duration = int(row[2]) - (int(row[2]) % 100)
            if duration not in data:
                data[duration] = 1
            else:
                data[duration] += 1

    with open(os.path.join(folder_path, ws + '_open_door_peak_by_duration.csv'), 'wb') as merged_file:
        merged_writer = csv.writer(merged_file)
        merged_writer.writerow(['Duration', 'Amount'])
        for key, value in sorted(data.items()):
            merged_writer.writerow([key/1000.0, value])

    print(ws + ' done.')

print("done.")
