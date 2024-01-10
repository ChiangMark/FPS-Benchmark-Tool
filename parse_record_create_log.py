import os
import csv
from collections import defaultdict
import re

folder_path = ".\\logs"
LOG_START_TIME = 0
LOG_END_TIME = 24

class parse_record_create_log:
    def __init__(self, wsName, startTime, endTime):
        self.results = {}
        self.notConfident = set()
        self.higherEnough = set()
        self.cacheWithoutEvent = set()
        self.startTime = startTime
        self.endTime = endTime

    def checkTime(self, logTime):
        hour = int(logTime.split(':')[0])
        return True if (hour>=self.startTime and hour<self.endTime) else False

    def parseLine(self, line):
        pattern_create = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* INFO\s+RequestLogFilter - \[([^\]]+)\] IP:.*METHOD:POST URI:.*record\.create PostParams.*'
        pattern_modified = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* DEBUG ExternalNotifier - \[([^\]]+)\] <RECORD_MODIFIED> start to send event'
        pattern_recent = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* DEBUG RecordDao - \[([^\]]+)\] PID:\d+ had record in short period: rID:(\d+), took'
        pattern_not_confident = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* WARN  WorkstationRecordApi - \[([^\]]+)\] record face template is not confident'
        pattern_higher_enough = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* INFO  WorkstationRecordApi - \[([^\]]+)\] record score is higher enough'
        pattern_cache = r'(\d{2}:\d{2}:\d{2}\.\d{3}) .* DEBUG WorkstationRecordApi - \[([^\]]+)\] checkRecordCache took'

        create_match = re.search(pattern_create, line)
        modified_match = re.search(pattern_modified, line)
        recent_match = re.search(pattern_recent, line)
        not_confident_match = re.search(pattern_not_confident, line)
        higher_enough_match = re.search(pattern_higher_enough, line)
        cache_match = re.search(pattern_cache, line)

        if create_match:
            timestamp, event_id = create_match.groups()
            if not self.checkTime(timestamp):
                return
            if event_id not in self.results:
                self.results[event_id] = {'create_time': timestamp, 'modified_time': None}
        elif modified_match:
            timestamp, event_id = modified_match.groups()
            if not self.checkTime(timestamp):
                return
            if event_id in self.results and self.results[event_id]['modified_time'] is None:
                self.results[event_id]['modified_time'] = timestamp
            if event_id in self.higherEnough:
                self.higherEnough.remove(event_id)
            if event_id in self.cacheWithoutEvent:
                self.cacheWithoutEvent.remove(event_id)
        elif recent_match:
            timestamp, request_id, record_id = recent_match.groups()
            if not self.checkTime(timestamp):
                return
            if request_id in self.results:
                del self.results[request_id]
        elif not_confident_match:
            timestamp, request_id = not_confident_match.groups()
            if not self.checkTime(timestamp):
                return
            if request_id in self.results:
                self.notConfident.add(request_id)
        elif higher_enough_match:
            timestamp, request_id = higher_enough_match.groups()
            if not self.checkTime(timestamp):
                return
            if request_id in self.results:
                self.higherEnough.add(request_id)
        elif cache_match:
            timestamp, request_id = cache_match.groups()
            if not self.checkTime(timestamp):
                return
            self.cacheWithoutEvent.add(request_id)


    def parseFile(self, filepath):
        with open(filepath, 'r') as f:
            for line in f.readlines():
                self.parseLine(line)

def write_record_file(filename, records, notConfident, higherEnough, cacheWithoutEvent):
    # print(len(records))
    # print(notConfident)
    # print(higherEnough)
    # print(cacheWithoutEvent)
    header = ['workstation', 'ticket', 'time', 'not confident', 'higher enough', 'cached']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        for record in records:
            if record[1] in notConfident:
                record.append("o")
            else:
                record.append("")
            if record[1] in higherEnough:
                record.append("o")
            else:
                record.append("")
            if record[1] in cacheWithoutEvent:
                record.append("o")
            else:
                record.append("")
            csv_writer.writerow(record)
    
## for single file testing
# results = []
# parser = parse_ws_log("A5_1")
# parser.parseFile("1025\\a1_1\\faceme.log")
# for event_id, times in sorted(parser.results.items(), key=lambda x:x[1]):
#     if times['modified_time'] is None:
#         results.append([event_id, str(times['create_time']).split('.')[0]])
# write_record_file("record_not_match.csv", results)


def find_log_files(folder_path):
    log_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".log") and file.startswith("faceme"):
                log_files.append(os.path.join(root, file))
    return log_files

workstations = []
for entry in os.listdir(folder_path):
    if os.path.isdir(os.path.join(folder_path, entry)):
        workstations.append(entry)
print(workstations)

results=[]
notConfident = set()
higherEnough = set()
cacheWithoutEvent = set()
for workstation in workstations:
    workstation_path = os.path.join(folder_path, workstation)
    log_files = find_log_files(os.path.join(folder_path, workstation))
    for log_file in sorted(log_files, reverse=True):
        print("processing file " + log_file)
        parser = parse_record_create_log(workstation, LOG_START_TIME, LOG_END_TIME)
        parser.parseFile(log_file)
        for event_id, times in sorted(parser.results.items(), key=lambda x:x[1]):
            if times['modified_time'] is None:
                results.append([workstation, event_id, str(times['create_time']).split('.')[0]])
        notConfident = notConfident.union(parser.notConfident)
        higherEnough = higherEnough.union(parser.higherEnough)
        cacheWithoutEvent = cacheWithoutEvent.union(parser.cacheWithoutEvent)

write_record_file("record_not_match.csv", results, notConfident, higherEnough, cacheWithoutEvent)


