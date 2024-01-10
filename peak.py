from collections import defaultdict
import math

class peak:
    def __init__(self):
        pass
    
    def calculateLogTimePeak(self, recordArrByWs, type):
        peakDict = None
        if type == 'minute':
            peakDict = defaultdict(lambda: defaultdict(int))
        elif type == 'second':
            peakDict = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        else: 
            print('undefined type')
            return None
    
        for records in recordArrByWs:
            for record in records:
                if record.t1 == None:
                    continue
                hour = int(record.t1.hour)
                minute = int(record.t1.minute)
                second = int(record.t1.second)
                if type == 'minute':
                    peakDict[hour][minute] += 1
                elif type == 'second':
                    peakDict[hour][minute][second] += 1
        return peakDict

    def calculateRecordCreateProcessTimePeak(self, recordArrByWs):
        peakDict = defaultdict(int)
        for i in range(40):
            peakDict[round(0.1*i, 1)] = 0

        for records in recordArrByWs:
            for record in records:
                if record.t1 == None:
                    continue
                delay = (record.t1-record.t01).total_seconds()
                delay = round(math.ceil(delay*10)/10, 1)
                peakDict[delay] += 1
        return peakDict