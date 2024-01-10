import csv
from collections import defaultdict
from datetime import datetime, timedelta
import os

folder_path = ".\\logs"
data = defaultdict(lambda: {'count': 0, 'total_duration': 0, 'max_duration': 0})

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
            try:
                frame_time = datetime.strptime(row[9], '%Y-%m-%d %H:%M:%S.%f')
            except:
                frame_time = datetime.strptime(row[9], '%Y-%m-%d %H:%M:%S')
            duration = int(row[2])
            key = (frame_time.date(), frame_time.strftime('%H:%M'))
            data[key]['count'] += 1
            data[key]['total_duration'] += duration
            data[key]['max_duration'] = max(data[key]['max_duration'], duration)

    with open(os.path.join(folder_path, ws + '_open_door_time_by_min.csv'), 'wb') as merged_file:
        merged_writer = csv.writer(merged_file)
        merged_writer.writerow(['Date', 'Time(per 1 min)', 'Total People Traffic', 'Average Process Time (s)', 'Highest Process Time (s)'])
        for key, value in sorted(data.items()):
            date, time = key
            count = value['count']
            total_duration = value['total_duration']
            max_duration = value['max_duration']
            average_duration = total_duration / count if count > 0 else 0
            time_range = "{}-{}".format(time, (datetime.strptime(time, '%H:%M') + timedelta(minutes=1)).strftime('%H:%M'))
            merged_writer.writerow([date, time_range, count, average_duration / 1000.0, max_duration / 1000.0])

    print(ws + ' done.')

print("done.")
