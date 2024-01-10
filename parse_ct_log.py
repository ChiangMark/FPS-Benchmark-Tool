import os
import csv
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

from peak import peak
from record import Record, Update, RecognizeTime, RecordUpdate
from utils import unzipfile 

folder_path = ".\\logs"
T01_T0_FILTER = 2000
T2_T2R_FILTER = 100
LOG_START_TIME = 0
LOG_END_TIME = 24


with open('time.txt', 'r') as file:
    # start_time
    date_time_str_first_line = file.readline().strip()
    start_time = datetime.strptime(date_time_str_first_line, '%Y/%m/%d %H:%M:%S.%f')
    # end_time
    date_time_str_second_line = file.readline().strip()
    end_time = datetime.strptime(date_time_str_second_line, '%Y/%m/%d %H:%M:%S.%f')

QUERYORINSERT_START_TIME = start_time.strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]
QUERYORINSERT_END_TIME = end_time.strftime('%Y/%m/%d %H:%M:%S.%f')[:-3]   # '2023/12/20 08:40:00'
RECORD_CREATE_SUMMARIZE_USE_QUERYORINSERT_TIME = True
T01_T0_INTERVAL = [2, 5, 10]    # [1.5, 2, 5]

class parse_ct_log:
    def __init__(self, wsName, startTime, endTime):
        self.processors = [self.processType0, self.processType1, self.processType2, self.processType3, self.processType4, self.processType5, self.processType6, self.processType7, self.processType8]
        self.wsName = wsName
        self.updateTicket = set()
        self.recordCreateArr = []
        self.ticketIndexDict = {}   # Find record index in recordCreateArr by ticket
        self.recordIdIndexDict = {} # Find record index in recordCreateArr by recordId
        self.dateFormat = '%Y/%m/%d %H:%M:%S.%f'
        self.startTime = startTime
        self.endTime = endTime
        self.queryorinsertStartTime = datetime.strptime(QUERYORINSERT_START_TIME, '%Y/%m/%d %H:%M:%S.%f')
        self.queryorinsertEndTime = datetime.strptime(QUERYORINSERT_END_TIME, '%Y/%m/%d %H:%M:%S.%f') #if QUERYORINSERT_END_TIME else None
        self.queryorinsertList = []
        self.queryorinsertTicketDict = dict()
    
    def processType0(self, infos):
        # print(infos)
        date = infos[0]
        time = infos[1]

        logTime = (' ').join([date, time])
        logTime = datetime.strptime(logTime, self.dateFormat)

        if RECORD_CREATE_SUMMARIZE_USE_QUERYORINSERT_TIME:
            if logTime < self.queryorinsertStartTime:
                return
            if self.queryorinsertEndTime and logTime > self.queryorinsertEndTime:
                return

        ticket = infos[6].strip("[]")
        cameraId = int(infos[13].split("=")[1])
        personId = int(infos[12].split('=')[1])
        try:
            faceId = int(infos[11].split('=')[1])
            score = float(infos[20].split('=')[1])
            occlusion = int(infos[21].split('=')[1])
            metadata = json.loads(infos[22].split('=')[1])
            boundingBox = metadata.get('boundingBox')
            boundingBoxStr = str(boundingBox)
            width = boundingBox[2] - boundingBox[0]
        except:
            faceId = None
            score = None
            occlusion = None
            boundingBox = None
            width = None

        t01 = int(infos[15].split('=')[1])
        second = t01 / 1000
        milliseconds = t01 % 1000
        t01 = datetime.fromtimestamp(second) + timedelta(milliseconds=milliseconds)

        t1 = (' ').join([date, time])
        t1 = datetime.strptime(t1, self.dateFormat)

        newRecord = Record()
        newRecord.logDate = date
        newRecord.ticket = ticket
        newRecord.cameraId = cameraId
        newRecord.personId = personId
        newRecord.faceId = faceId
        newRecord.score = score
        newRecord.boundingBox = boundingBox
        newRecord.width = width
        newRecord.occlusion = occlusion
        newRecord.t01 = t01
        newRecord.t1 = t1
        self.recordCreateArr.append(newRecord)
        self.ticketIndexDict[ticket] = len(self.recordCreateArr)-1

    def processType1(self, infos):
        ticket = infos[6].strip("[]")
        recordId = int(infos[9].strip(","))
        if ticket not in self.ticketIndexDict:
            print(ticket + ' not in record index dict')
            return
        index = self.ticketIndexDict[ticket]
        self.recordCreateArr[index].recordId = recordId
        self.recordIdIndexDict[recordId] = index

    def processType2(self, infos):
        date = infos[0]
        time = infos[1]
        ticket = infos[6].strip("[]")

        t2 = (' ').join([date, time])
        t2 = datetime.strptime(t2, self.dateFormat)

        if ticket not in self.ticketIndexDict:
            print(ticket + ' not in record index dict')
            return
        index = self.ticketIndexDict[ticket]
        self.recordCreateArr[index].t2 = t2

    def processType3(self, infos):
        date = infos[0]
        time = infos[1]
        ticket = infos[6].strip("[]")

        t2r = (' ').join([date, time])
        t2r = datetime.strptime(t2r, self.dateFormat)

        if ticket not in self.ticketIndexDict:
            print(ticket + ' not in record index dict')
            return
        index = self.ticketIndexDict[ticket]
        self.recordCreateArr[index].t2r = t2r
        self.recordCreateArr[index].status = 'success'
        del self.ticketIndexDict[ticket]

    def processType4(self, infos):
        date = infos[0]
        time = infos[1]
        ticket = infos[6].strip("[]")

        t2r = (' ').join([date, time])
        t2r = datetime.strptime(t2r, self.dateFormat)

        if ticket not in self.ticketIndexDict:
            print(ticket + ' not in record index dict')
            return
        index = self.ticketIndexDict[ticket]
        self.recordCreateArr[index].t2r = t2r
        self.recordCreateArr[index].status = 'connect error'
        del self.ticketIndexDict[ticket]

    def processType5(self, infos):
        date = infos[0]
        time = infos[1]
        ticket = infos[6].strip("[]")

        t2r = (' ').join([date, time])
        t2r = datetime.strptime(t2r, self.dateFormat)

        if ticket not in self.ticketIndexDict:
            print(ticket + ' not in record index dict')
            return
        index = self.ticketIndexDict[ticket]
        self.recordCreateArr[index].t2r = t2r
        self.recordCreateArr[index].status = 'response NG'
        del self.ticketIndexDict[ticket]
    
    def processType6(self, infos):
        date = infos[0]
        time = infos[1]
        logTime = (' ').join([date, time])
        logTime = datetime.strptime(logTime, self.dateFormat)
        personId = int(infos[12].split('=')[1])
        recordId = int(infos[13].split('=')[1])
        try:
            faceId = int(infos[11].split('=')[1])
            score = float(infos[18].split('=')[1])
            occlusion = int(infos[19].split('=')[1])
            metadata = json.loads(infos[20].split('=')[1])
            boundingBox = metadata.get('boundingBox')
            boundingBoxStr = str(boundingBox)
            width = boundingBox[2] - boundingBox[0]
        except:
            faceId = None
            score = None
            boundingBox = None
            width = None
            occlusion = None

        newUpdate = Update()
        newUpdate.personId = personId
        newUpdate.logDate = logTime
        newUpdate.faceId = faceId
        newUpdate.score = score
        newUpdate.boundingBox = boundingBox
        newUpdate.width = width
        newUpdate.occlusion = occlusion
        
        if recordId not in self.recordIdIndexDict:
            print('Record update: ' + str(recordId) + ', but record create not found. Skip.')
            return 
        index = self.recordIdIndexDict[recordId]
        self.recordCreateArr[index].updatedRecord.append(newUpdate)

    def processType7(self, infos):
        date = infos[0]
        time = infos[1]
        ticket = infos[6].strip("[]")
        logTime = (' ').join([date, time])
        logTime = datetime.strptime(logTime, self.dateFormat)

        count = 0
        for info in infos:
            if 'faceFeatures=<BLOB>' == info:
                count += 1

        self.queryorinsertTicketDict[ticket] = [logTime, count, 0]

        if logTime >= self.queryorinsertStartTime:
            if not self.queryorinsertEndTime or logTime <= self.queryorinsertEndTime:
                self.queryorinsertList.append((logTime, count))

    def processType8(self, infos):
        ticket = infos[6].strip("[]")
        try:
            timeNumber = infos[11]
            timeUnit = infos[12]
            if timeUnit == 'ms':
                spendTime = float(timeNumber)
            elif timeUnit == 's':
                spendTime = float(timeNumber) * 1000
            elif timeUnit == 'min':
                spendTime = float(timeNumber) * 1000 * 60
            else:
                spendTime = float(timeNumber)

            if ticket in self.queryorinsertTicketDict.keys():
                self.queryorinsertTicketDict[ticket][2] = spendTime
        except Exception as e:
            pass

    def getInfoType(self, infos):
        ## example
        # type0: 2023/11/03 05:42:48.802 [qtp1530388690-19247] INFO  RequestLogFilter - [jh4rb9] IP:127.0.0.1 METHOD:POST URI:/api/workstation/record.create PostParams:[ faceId=140016 personId=273720 camId=155 workstationId=9 logTime=1698961367733 originalLogTime=1695618705379 snapshot=<BLOB> featureType=10 featureSubType=2 score=88.000000 occlusion=0 metadata={"boundingBox":[46,284,136,394],"height":1080,"width":1920} featureOrder=BIG_ENDIAN faceFeature=<BLOB> ] AUTH:ljOVB*****yVkUu
        # type1: 2023/11/03 05:42:48.802 [qtp1530388690-19247] DEBUG WorkstationRecordApi - [jh4rb9] record created: 1792221, took 27.70 ms
        # type2: 2023/11/03 05:42:48.884 [e-10.5.16.116:5008-4] DEBUG ExternalNotifier - [jh4rb9] <RECORD_MODIFIED> start to send event
        # type3: 2023/11/04 00:04:36.015 [e-10.5.16.116:5008-3] DEBUG ExternalNotifier - [jh4rb9] <RECORD_MODIFIED> targetId: 1792221, took 34.20 ms
        # type4: 2023/11/03 05:42:53.892 [e-10.5.16.116:5008-4] ERROR ExternalNotifier - [jh4rb9] cannot connect to peer and took 5.036 s, msg: java.net.SocketTimeoutException: Read timed out
        # type5: 2023/11/03 05:42:53.892 [e-10.5.16.116:5008-4] ERROR ExternalNotifier - [jh4rb9] response NG: *
        # type6: 2023/11/04 00:04:36.340 [qtp1530388690-26461] INFO  RequestLogFilter - [EkVtTf] IP:127.0.0.1 METHOD:POST URI:/api/workstation/record.update PostParams:[ faceId=190079 personId=427895 recordId=1792221 updateTime=1699027475297 snapshot=<BLOB> featureType=10 featureSubType=2 score=96.000000 occlusion=4 metadata={"boundingBox":[1271,246,1343,316],"height":1080,"width":1920} featureOrder=BIG_ENDIAN faceFeature=<BLOB> ] AUTH:ljOVB*****yVkUu
        # type7: 2023/12/21 17:21:33.339 [qtp1401420256-197] INFO  RequestLogFilter - [DFDsSh] IP:192.168.27.36 METHOD:POST URI:/api/workstation/face.queryorinsert PostParams:[ featureType=10 featureSubType=2 preciseLevel=5 autoCreates=true occlusions=0 frame_id_=1083 scores=83.000000 featureOrder=BIG_ENDIAN faceFeatures=<BLOB> autoCreates=true occlusions=0 frame_id_=926 scores=83.000000 featureOrder=BIG_ENDIAN faceFeatures=<BLOB> ] AUTH:QyN09*****FZmUu
        # type8: 2023/12/27 15:00:42.075 [qtp1401420256-62] INFO  ResponseLogFilter - [daQ4b1] Status: 200 OK, Time: 3.004 ms
        warningTypeDict = {
            "FaceProcessSequenceStatus::GetLoginFace": 0,
            "DataUploader::DoQueryData": 1,
            "DataUploader::DoUploadData": 2,
            "CameraInfo::OnVideoFrame": 3
        }
        # print(infos)
        status = infos[3]
        event = infos[4]
        ticket = infos[6].strip("[]")
        if status=='INFO' and event=='RequestLogFilter':
            if infos[9].endswith('/record.create'):
                return 0
            elif infos[9].endswith('/record.update'):
                self.updateTicket.add(ticket)
                return 6
            elif infos[9].endswith('/face.queryorinsert'):
                return 7
        elif status=='INFO' and event=='ResponseLogFilter':
            if infos[7].endswith('Status:'):
                return 8
        elif event=='ExternalNotifier':
            if status=='DEBUG':
                if infos[8]=='start':
                    return 2
                elif infos[8]=='targetId:':
                    return 3
            elif status=='ERROR':
                if infos[7]=='cannot':
                    return 4
                elif infos[7]=='response' and infos[8]=='NG':
                    return 5
        elif event=='WorkstationRecordApi':
            if infos[7] == 'record' and infos[8] == 'created:':
                return 1
        return -1
    
    def checkTime(self, logTime):
        try:
            hour = int(logTime.split(':')[0])
        except:
            return False
        return True if (hour>=self.startTime and hour<self.endTime) else False

    def parseLine(self, line):
        if line.startswith('['):
            return
        infos = line.split()
        if len(infos)<10:
            return
        ticket = infos[6].strip("[]")
        if not self.checkTime(infos[1]):
            return
        infoType = self.getInfoType(infos)
        if infoType<0 or (ticket in self.updateTicket and infoType != 6):
            return
        self.processors[infoType](infos)

    def parseFile(self, filepath):
        with open(filepath, 'r') as f:
            for line in f.readlines():
                self.parseLine(line)

def write_record_create_summarize_file(outputFile, workstations, recordArrByWs, notMatchDictByWs):
    level0 = '0~' + str(T01_T0_INTERVAL[0]) + ' s'
    level1 = str(T01_T0_INTERVAL[0]) + '~' + str(T01_T0_INTERVAL[1]) + ' s'
    level2 = str(T01_T0_INTERVAL[1]) + '~' + str(T01_T0_INTERVAL[2]) + ' s'
    level3 = '>=' + str(T01_T0_INTERVAL[2]) + ' s'
    t01Header = ['WS', 'Total amount', 'Avg. (s)', 'Max. (s)', level0, level1, level2, level3]
    t1Header = ['WS', 'Total amount', 'Merged / Not confident', 'Avg. (ms)', 'Max. (ms)', '<100 ms', '100~200 ms', '200~500 ms', '>500 ms']
    t2Header = ['WS', 'Total amount', 'Avg. (ms)', 'Max. (ms)', '<100 ms', '100~1000 ms', '1~2 s', '2~5 s', '>5 s']
    t01Body, t1Body, t2Body = [], [], []

    for i, workstation in enumerate(workstations):
        records = recordArrByWs[i]
        totalCount = len(recordArrByWs[i])
        t01content = [workstation, totalCount, 0, 0, 0, 0, 0, 0]                             # total count, sum, max, T01_T0_INTERVAL
        t1content = [workstation, totalCount, len(notMatchDictByWs[i]), 0, 0, 0, 0, 0, 0]
        t2content = [workstation, totalCount, 0, 0, 0, 0, 0, 0, 0]
        for record in records:
            if record.t1 == None:
                continue
            t01 = int((record.t1-record.t01).total_seconds() * 1000)
            t01content[2] += t01
            t01content[3] = max(t01, t01content[3])
            if t01 <= (T01_T0_INTERVAL[0] * 1000):
                t01content[4] += 1
            elif t01 <= (T01_T0_INTERVAL[1] * 1000):
                t01content[5] += 1
            elif t01 <= (T01_T0_INTERVAL[2] * 1000):
                t01content[6] += 1
            else:
                t01content[7] += 1
            
            if record.t2 == None:
                continue
            t1 = int((record.t2-record.t1).total_seconds() * 1000)
            t1content[3] += t1
            t1content[4] = max(t1, t1content[4])
            if t1<100:
                t1content[5] += 1
            elif t1<200:
                t1content[6] += 1
            elif t1<500:
                t1content[7] += 1
            else:
                t1content[8] += 1

            if record.t2r == None:
                continue
            t2 = int((record.t2r-record.t2).total_seconds() * 1000)
            t2content[2] += t2
            t2content[3] = max(t2, t2content[3])
            if t2<100:
                t2content[4] += 1
            elif t2<1000:
                t2content[5] += 1
            elif t2<2000:
                t2content[6] += 1
            elif t2<5000:
                t2content[7] += 1
            else:
                t2content[8] += 1
        
        t01content[2] = round((float(t01content[2])/1000) / t01content[1], 3) if t01content[2]>0 else 0
        t01content[3] = round(float(t01content[3])/1000, 3)
        t1content[3] = round(float(t1content[3]) / t1content[1], 2) if t1content[3]>0 else 0
        t2content[2] = round(float(t2content[2]) / t2content[1], 2) if t2content[2]>0 else 0
        t01Body.append(t01content)
        t1Body.append(t1content)
        t2Body.append(t2content)
    
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(t01Header)
        for body in t01Body:
            csv_writer.writerow(body)
        csv_writer.writerow([])

        csv_writer.writerow(t1Header)
        for body in t1Body:
            csv_writer.writerow(body)
        csv_writer.writerow([])

        csv_writer.writerow(t2Header)
        for body in t2Body:
            csv_writer.writerow(body)
        csv_writer.writerow([])

def write_record_create_byWs(outputFile, records, time):
    header = ['ticket', 'log time', 'cameraId', 'delay time (ms)']
    results = []

    for record in records:
        if record.t1 == None:
            continue
        delay = int((record.t1-record.t01).total_seconds() * 1000)
        if delay>time:
            results.append([record.ticket, record.t1, record.cameraId, delay])
    
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        for result in results:
            csv_writer.writerow(result)

def write_notification_response_byWs(outputFile, records, time):
    header = ['ticket', 'log time', 'cameraId', 'delay time (ms)']
    results = []

    for record in records:
        if record.t2r == None or record.t2 == None:
            continue
        delay = int((record.t2r-record.t2).total_seconds() * 1000)
        if delay>time:
            results.append([record.ticket, record.t1, record.cameraId, delay])
    
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        for result in results:
            csv_writer.writerow(result)
            
def write_recognize_time_byWs(outputFile, records):
    def getBoundingBoxCenter(boundingBox):
        return [(boundingBox[0]+boundingBox[2])//2, (boundingBox[1]+boundingBox[3])//2]

    def isOutlier(boundingBox):
        outlierUpperX = int(1920*0.8)
        outlierLowerX = int(1920*0.2)
        outlierUpperY = int(1080*0.8)
        outlierLowerY = int(1080*0.1)
        centerX, centerY = getBoundingBoxCenter(boundingBox)

        return not (centerX>outlierLowerX and centerX<outlierUpperX and centerY>outlierLowerY and centerY<outlierUpperY)

    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['recordId', 'cameraId', 'duration', 'rec score', 'occlusion', 'rec width', 'create/update count', 'outlier count', 'outlier rate', 'frame time',
                              'create time', 'personId', 'faceId', 'score', 'occlusion', 'boundingBox', 'isOutlier', 'width', 
                              'update time', 'personId', 'faceId', 'score', 'occlusion', 'boundingBox', 'isOutlier', 'width', 
                              'update time', 'personId', 'faceId', 'score', 'occlusion', 'boundingBox', 'isOutlier', 'width'])
        cameraRecordDict = defaultdict(lambda: [])
        cameraRecordDictOutput = defaultdict(lambda: [])    # Output for function write_recognize_time_byCamId_byWs() 

        for record in records:
            outlierCount = 0
            updateCount = 1
            if not record.boundingBox:
                continue
            if isOutlier(record.boundingBox):
                outlierCount += 1

            targetPersonId = record.updatedRecord[-1].personId if len(record.updatedRecord)>0 else None
            targetRecord = None
            if targetPersonId != record.personId:
                for update in  record.updatedRecord:
                    updateCount += 1
                    if isOutlier(update.boundingBox):
                        outlierCount += 1
                    if update.personId == targetPersonId:
                        targetRecord = update
                        break

            if len(record.updatedRecord) == 0 or targetPersonId == record.personId:
                delay = int((record.t1 - record.t01).total_seconds() * 1000)
                score = record.score
                width = record.width
                occlusion = record.occlusion
                updateCount = 1
            else:
                delay = int((targetRecord.logDate - record.t01).total_seconds() * 1000)
                score = targetRecord.score
                width = targetRecord.width
                occlusion = record.occlusion

            #     for update in record.updatedRecord:
            #         if update.personId != curPersonId:
            #             curPersonId = update.personId
            #             lastTime = update.logDate

            #     if curPersonId == firstPersonId:
            #         delay = int((record.t1 - record.t01).total_seconds() * 1000)
            #     else:
            #         delay = int((lastTime - record.t01).total_seconds() * 1000)
            row = [record.recordId, record.cameraId, delay, score, occlusion, width, updateCount, outlierCount, float(outlierCount) / updateCount,
                   record.t01, record.t1, record.personId, record.faceId, record.score, record.occlusion, record.boundingBox, isOutlier(record.boundingBox),record.width]
            if targetPersonId != record.personId:
                for update in record.updatedRecord:
                    row.append(update.logDate)
                    row.append(update.personId)
                    row.append(update.faceId)
                    row.append(update.score)
                    row.append(update.occlusion)
                    row.append(update.boundingBox)
                    row.append(isOutlier(update.boundingBox))
                    row.append(update.width)
                    if update.personId == targetPersonId:
                        break
            # csv_writer.writerow(row)
            cameraRecordDict[record.cameraId].append(row)

            recordUpdate = RecordUpdate()
            recordUpdate.record = record
            recordUpdate.updateCount = updateCount
            recordUpdate.duration = delay
            recordUpdate.outlierCount = outlierCount
            recordUpdate.faceWidth = width
            recordUpdate.score = score
            cameraRecordDictOutput[record.cameraId].append(recordUpdate)

        for cameraId in sorted(cameraRecordDict.keys()):
            for record in cameraRecordDict[cameraId]:
                csv_writer.writerow(record)

        return cameraRecordDictOutput

def write_recognize_time_byCamId_byWs(outputFile, cameraRecordDict):
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['cameraId', 'record count', 'w/o update count', 'w update count', 'avg duration (ms)', 'max duration (ms)', 'avg create/udpate count', 'max create/udpate count',
                             '>2s', '>2s (%)', '>2s & outlier rate>=0.5', '>2s & outlier rate>=0.5 (%)', '>2s & face width<5%', '>2s & face width<5% (%)',
                              '>2s & QC<82', '>2s & QC<82 (%)', '>2s & mask', '>2s & mask (%)',
                             '>4s', '>4s (%)', '>4s & outlier rate>=0.5', '>4s & outlier rate>=0.5 (%)', '>4s & face width<5%', '>4s & face width<5% (%)',
                              '>4s & QC<82', '>4s & QC<82 (%)', '>4s & mask', '>4s & mask (%)',])
        
        for cameraId in sorted(cameraRecordDict.keys()):
            records = cameraRecordDict[cameraId]
            recordCount = len(records)
            woUpdateCount, wUpdateCount, totalDuration, maxDuration, createUpdateSum, createUpdateMax = 0, 0, 0, 0, 0, 0
            delay2s, outlierCount2s, faceWidthCount2s, qcCount2s, mask2s = 0, 0, 0, 0, 0
            delay4s, outlierCount4s, faceWidthCount4s, qcCount4s, mask4s = 0, 0, 0, 0, 0
            for record in records:
                updateCount = record.updateCount
                duration = record.duration

                if updateCount==1:
                    woUpdateCount += 1
                else:
                    wUpdateCount += 1
                createUpdateSum += updateCount
                createUpdateMax = max(createUpdateMax, updateCount)
                totalDuration += duration
                maxDuration = max(maxDuration, duration)
                
                if duration>2000:
                    delay2s += 1
                    if (float(record.outlierCount)/float(record.updateCount)>0.5):
                        outlierCount2s += 1
                    if record.record.width<96:
                        faceWidthCount2s += 1
                    if record.record.score<82:
                        qcCount2s += 1
                    if record.record.occlusion & 16:
                        mask2s += 1
                if duration>4000:
                    delay4s += 1
                    if (float(record.outlierCount)/float(record.updateCount)>0.5):
                        outlierCount4s += 1
                    if record.record.width<96:
                        faceWidthCount4s += 1
                    if record.record.score<82:
                        qcCount4s += 1
                    if record.record.occlusion & 16:
                        mask4s += 1
                
            
            csv_writer.writerow([cameraId, recordCount, woUpdateCount, wUpdateCount, round(float(totalDuration)/float(recordCount), 2), maxDuration, round(float(createUpdateSum)/float(recordCount), 2), createUpdateMax, 
                                 delay2s, str(round(float(delay2s*100)/recordCount, 2))+' %', outlierCount2s, str(round(float(outlierCount2s*100)/recordCount, 2))+' %', 
                                 faceWidthCount2s, str(round(float(faceWidthCount2s*100)/recordCount, 2))+' %', qcCount2s, str(round(float(qcCount2s*100)/recordCount, 2))+' %',
                                 mask2s, str(round(float(mask2s*100)/recordCount, 2))+' %',
                                 delay4s, str(round(float(delay4s*100)/recordCount, 2))+' %', outlierCount4s, str(round(float(outlierCount4s*100)/recordCount, 2))+' %', 
                                 faceWidthCount4s, str(round(float(faceWidthCount4s*100)/recordCount, 2))+' %', qcCount4s, str(round(float(qcCount4s*100)/recordCount, 2))+' %',
                                 mask4s, str(round(float(mask4s*100)/recordCount, 2))+' %',])



            
def write_recognize_time_create_update_byWs(outputFile, records):
    createFile = outputFile + '_recognize_time_create.csv'
    updateFile = outputFile + '_recognize_time_update.csv'
    createOutput = []
    updateOutput = []
    createSum, updateSum = 0, 0
    createMax, updateMax = -float('inf'), -float('inf')
    createt0_2, update0_2 = 0, 0
    createt2_4, updatet2_4 = 0, 0
    createt4_10, updatet4_10 = 0, 0
    createt10_, updatet10_ = 0, 0

    for record in records:
        targetPersonId = record.updatedRecord[-1].personId if len(record.updatedRecord)>0 else None
        targetRecord = None
        if targetPersonId != record.personId:
            for update in  record.updatedRecord:
                if update.personId == targetPersonId:
                    targetRecord = update
                    break

        if len(record.updatedRecord)==0 or targetPersonId == record.personId:
            delay = int((record.t1-record.t01).total_seconds()*1000)
            createOutput.append([record.recordId, record.t1, delay])
            createMax = max(delay, createMax)
            createSum += delay
            if delay<=2000:
                createt0_2 += 1
            elif delay<=4000:
                createt2_4 += 1
            elif delay<=10000:
                createt4_10 += 1
            else:
                createt10_ += 1
        else:
            firstPersonId = record.personId
            curPersonId = record.personId
            lastTime = record.t1

            # for update in record.updatedRecord:
            #     if update.personId != curPersonId:
            #         curPersonId = update.personId
            #         lastTime = update.logDate

            # if curPersonId == firstPersonId:
            #     delay = int((record.t1-record.t01).total_seconds()*1000)
            #     createOutput.append([record.recordId, record.t1, delay])
            #     createMax = max(delay, createMax)
            #     createSum += delay
            # else:
            createDelay = int((record.t1-record.t01).total_seconds()*1000)
            updateDelay = int((targetRecord.logDate-record.t01).total_seconds()*1000)
            updateOutput.append([record.recordId, record.t01, createDelay, updateDelay])
            updateMax = max(updateDelay, updateMax)
            updateSum += updateDelay
            
            if updateDelay<=2000:
                update0_2 += 1
            elif updateDelay<=4000:
                updatet2_4 += 1
            elif updateDelay<=10000:
                updatet4_10 += 1
            else:
                updatet10_ += 1

    # print('create record')
    # for output in createOutput:
    #     print(output)
    # print('================================================')
    # # print('update record')
    # for output in updateOutput:
    #     print(output)
    
    with open(createFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['recordId', 'frame time', 'create duration'])
        for record in createOutput:
            csv_writer.writerow(record)

    with open(updateFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['recordId', 'frame time', 'create duration', 'udpate duration'])
        for record in updateOutput:
            csv_writer.writerow(record)
    
    createTime, updateTime = RecognizeTime(), RecognizeTime()
    createTime.amount = len(createOutput)
    createTime.summation = createSum
    createTime.maximum = createMax
    createTime.t0_2 = createt0_2
    createTime.t2_4 = createt2_4
    createTime.t4_10 = createt4_10
    createTime.t10_ = createt10_

    updateTime.amount = len(updateOutput)
    updateTime.summation = updateSum
    updateTime.maximum = updateMax
    updateTime.t0_2 = update0_2
    updateTime.t2_4 = updatet2_4
    updateTime.t4_10 = updatet4_10
    updateTime.t10_ = updatet10_
    return [createTime, updateTime]

def write_recognize_time_summarize_file(outputFile, workstations, recognizeTime):
    # recognizeTime: [create recognize time, update recognize time]
    createUpdateTotal = []
    for i in range(len(workstations)):
        totalRecognizeTime = RecognizeTime()
        totalRecognizeTime.amount = recognizeTime[i][0].amount + recognizeTime[i][1].amount
        totalRecognizeTime.summation = recognizeTime[i][0].summation + recognizeTime[i][1].summation
        totalRecognizeTime.maximum = max(recognizeTime[i][0].maximum, recognizeTime[i][1].maximum)
        totalRecognizeTime.t0_2 = recognizeTime[i][0].t0_2 + recognizeTime[i][1].t0_2
        totalRecognizeTime.t2_4 = recognizeTime[i][0].t2_4 + recognizeTime[i][1].t2_4
        totalRecognizeTime.t4_10 = recognizeTime[i][0].t4_10 + recognizeTime[i][1].t4_10
        totalRecognizeTime.t10_ = recognizeTime[i][0].t10_ + recognizeTime[i][1].t10_
        createUpdateTotal.append(totalRecognizeTime)
    
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Total'])
        csv_writer.writerow(['WS', 'Total amount', 'Avg. (s)', 'Max. (s)', '0~2 s', '2~4 s', '4~10 s', '>=10 s'])
        for i, workstation in enumerate(workstations):
            amount = createUpdateTotal[i].amount
            summation = createUpdateTotal[i].summation
            avg = round(float(summation) / float(amount) / 1000, 2) if summation>0 else 0
            maxValue = round(createUpdateTotal[i].maximum / 1000, 2)
            csv_writer.writerow([workstation, amount, avg, maxValue, createUpdateTotal[i].t0_2, createUpdateTotal[i].t2_4, createUpdateTotal[i].t4_10, createUpdateTotal[i].t10_])
        amount = sum([x.amount for x in createUpdateTotal])
        summation = sum([x.summation for x in createUpdateTotal])
        maxValue = round(max([x.maximum for x in createUpdateTotal])/1000, 2) if createUpdateTotal else 0
        avg = round(float(summation)/float(amount)/1000, 2) if summation>0 else 0
        t0_2 = sum([x.t0_2 for x in createUpdateTotal])
        t2_4 = sum([x.t2_4 for x in createUpdateTotal])
        t4_10 = sum([x.t4_10 for x in createUpdateTotal])
        t10_ = sum([x.t10_ for x in createUpdateTotal])
        csv_writer.writerow(['Total', amount, avg, maxValue, t0_2, t2_4, t4_10, t10_])
        csv_writer.writerow([])

        csv_writer.writerow(['Create'])
        createTime = [x[0] for x in recognizeTime]
        csv_writer.writerow(['WS', 'Total amount', 'Avg. (s)', 'Max. (s)', '0~2 s', '2~4 s', '4~10 s', '>=10 s'])
        for i, workstation in enumerate(workstations):
            amount = createTime[i].amount
            summation = createTime[i].summation
            avg = round(float(summation) / float(amount)/1000, 2) if summation>0 else 0
            maxValue = round(createTime[i].maximum/1000, 2)
            csv_writer.writerow([workstation, amount, avg, maxValue, createTime[i].t0_2, createTime[i].t2_4, createTime[i].t4_10, createTime[i].t10_])
        amount = sum([x.amount for x in createTime])
        summation = sum([x.summation for x in createTime])
        maxValue = round(max([x.maximum for x in createTime])/1000, 2) if createTime else 0
        avg = round(float(summation)/float(amount)/1000, 2) if summation>0 else 0
        t0_2 = sum([x.t0_2 for x in createTime])
        t2_4 = sum([x.t2_4 for x in createTime])
        t4_10 = sum([x.t4_10 for x in createTime])
        t10_ = sum([x.t10_ for x in createTime])
        csv_writer.writerow(['Total', amount, avg, maxValue, t0_2, t2_4, t4_10, t10_])
        csv_writer.writerow([])
        
        csv_writer.writerow(['Update'])
        updateTime = [x[1] for x in recognizeTime]
        csv_writer.writerow(['WS', 'Total amount', 'Avg. (s)', 'Max. (s)', '0~2 s', '2~4 s', '4~10 s', '>=10 s'])
        for i, workstation in enumerate(workstations):
            amount = updateTime[i].amount
            summation = updateTime[i].summation
            avg = round(float(summation) / float(amount) / 1000, 2) if summation>0 else 0
            maxValue = round(updateTime[i].maximum/1000, 2)
            csv_writer.writerow([workstation, amount, avg, maxValue, updateTime[i].t0_2, updateTime[i].t2_4, updateTime[i].t4_10, updateTime[i].t10_])
        amount = sum([x.amount for x in updateTime])
        summation = sum([x.summation for x in updateTime])
        maxValue = round(max([x.maximum for x in updateTime])/1000, 2) if updateTime else 0
        avg = round(float(summation)/float(amount)/1000, 2) if summation>0 else 0
        t0_2 = sum([x.t0_2 for x in updateTime])
        t2_4 = sum([x.t2_4 for x in updateTime])
        t4_10 = sum([x.t4_10 for x in updateTime])
        t10_ = sum([x.t10_ for x in updateTime])
        csv_writer.writerow(['Total', amount, avg, maxValue, t0_2, t2_4, t4_10, t10_])

    
def write_record_peak(outputFile, peakData, type):
    header = ['door open time', 'door open count']
    results = []

    if type == 'minute':
        for hour in sorted(peakData.keys()):
            for minute in sorted(peakData[hour].keys()):
                results.append([str(hour) + ':' + str(minute), peakData[hour][minute]])
    elif type == 'second':
        for hour in sorted(peakData.keys()):
            for minute in sorted(peakData[hour].keys()):
                for second in sorted(peakData[hour][minute].keys()):
                    results.append([str(hour) + ':' + str(minute) + ':' + str(second), peakData[hour][minute][second]])
    elif type == 'process':
        for time in sorted(peakData.keys()):
            results.append([time, peakData[time]])
    
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        for result in results:
            csv_writer.writerow(result)

def write_queryorinsert_list_byWs(outputFile, queryorinsertTicketDict):
    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['log time', 'count', 'spend time'])

        for queryorinsertInfo in queryorinsertTicketDict.values():
            csv_writer.writerow(queryorinsertInfo)

def write_queryorinsert_summary_file(outputFile, workstations, queryorinsertListByWs):
    totalFirstTime = None
    totalLastTime = None
    totalCount = 0
    totalLastCount = 0
    wsQueryorinsertList = []

    for i, workstation in enumerate(workstations):
        queryorinsertList = queryorinsertListByWs[i]
        if not queryorinsertList or len(queryorinsertList) < 2:
            continue
        firstTime = None
        lastTime = None
        count = 0
        lastCount = 0
        for queryorinsertTime, queryorinsertCount in queryorinsertList:
            count += queryorinsertCount
            totalCount += queryorinsertCount
            if not firstTime or queryorinsertTime < firstTime:
                firstTime = queryorinsertTime
            if not lastTime or queryorinsertTime > lastTime:
                lastTime = queryorinsertTime
                lastCount = queryorinsertCount
            if not totalFirstTime or queryorinsertTime < totalFirstTime:
                totalFirstTime = queryorinsertTime
            if not totalLastTime or queryorinsertTime > totalLastTime:
                totalLastTime = queryorinsertTime
                totalLastCount = queryorinsertCount
        duration = (lastTime - firstTime).total_seconds()
        wsQueryorinsertList.append([workstation, firstTime, lastTime, duration, count, float(count - lastCount) / duration])

    with open(outputFile, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['WS', 'first time', 'last time', 'duration (s)', 'count', 'fps'])
        if wsQueryorinsertList:
            for info in wsQueryorinsertList:
                csv_writer.writerow(info)

            if totalCount > 1:
                csv_writer.writerow([])
                duration = (totalLastTime - totalFirstTime).total_seconds()
                csv_writer.writerow(['total', totalFirstTime, totalLastTime, duration, totalCount, float(totalCount - totalLastCount) / duration])

def find_log_files(folder_path):
    log_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".log") and "faceme" in file:
                log_files.append(os.path.join(root, file))
    return log_files

for root, dirs, files in os.walk(folder_path):
    for filename in files:
        if filename.endswith(".gz"):
            if filename[:filename.find(".gz")] not in files:
                unzipfile(os.path.join(root, filename))

wsDelayOutput = []
frameDelayOutput = []
workstations = []
for entry in os.listdir(folder_path):
    if os.path.isdir(os.path.join(folder_path, entry)):
        workstations.append(entry)
print("workstations:")
print(workstations)

recordArrByWs = []
notMatchDictByWs = []
recognizeTime = []
queryorinsertListByWs = []
for workstation in workstations:
    log_files = find_log_files(os.path.join(folder_path, workstation))
    log_files = sorted(log_files, key=lambda x: len(x), reverse=True)       #sorted by log file created time according to file name
    # print(log_files)
    parser = parse_ct_log(workstation, LOG_START_TIME, LOG_END_TIME)
    for log_file in log_files:
        print("processing file " + log_file)
        startTime = time.time()
        parser.parseFile(log_file)
        print("process " + log_file + " complete, took " + str(round(time.time()-startTime, 2)) + " seconds")
    print("record create count: " + str(len(parser.recordCreateArr)))
    print("not match count: " + str(len(parser.ticketIndexDict)))

    startTime = time.time()
    # print(parser.ticketIndexDict.keys())
    recordArrByWs.append(parser.recordCreateArr)
    notMatchDictByWs.append(parser.ticketIndexDict)
    queryorinsertListByWs.append(parser.queryorinsertList)

    outputFile = os.path.join(folder_path, workstation)
    outputFile += '_ct_t01_t1.csv'
    write_record_create_byWs(outputFile, parser.recordCreateArr, T01_T0_FILTER)

    outputFile = os.path.join(folder_path, workstation)
    outputFile += '_ct_t2_t2R.csv'
    write_notification_response_byWs(outputFile, parser.recordCreateArr, T2_T2R_FILTER)

    outputFile = os.path.join(folder_path, workstation)
    outputFile += '_recognize_time.csv'
    cameraRecordDict = write_recognize_time_byWs(outputFile, parser.recordCreateArr)

    outputFile = os.path.join(folder_path, workstation)
    outputFile += '_recognize_time_summarize_byCamId.csv'
    write_recognize_time_byCamId_byWs(outputFile, cameraRecordDict)

    outputFile = os.path.join(folder_path, workstation)
    recognizeTime.append(write_recognize_time_create_update_byWs(outputFile, parser.recordCreateArr))

    outputFile = os.path.join(folder_path, workstation)
    outputFile += '_queryorinsert.csv'
    write_queryorinsert_list_byWs(outputFile, parser.queryorinsertTicketDict)

    print("export report for workstation " + workstation + ", took " + str(round(time.time()-startTime, 2)) + " seconds")

startTime = time.time()
# write csv for record create delay summary
outputFile = os.path.join(folder_path, "recordCreateSummarize.csv")
write_record_create_summarize_file(outputFile, workstations, recordArrByWs, notMatchDictByWs) 

# write csv for record create peak per second (log time)
peakParser = peak()
peakBySecond = peakParser.calculateLogTimePeak(recordArrByWs, 'second')
outputFile = os.path.join(folder_path, "peakBySecond.csv")
write_record_peak(outputFile, peakBySecond, 'second')

# write csv for record create peak per minute (log time)
peakByMinute = peakParser.calculateLogTimePeak(recordArrByWs, 'minute')
outputFile = os.path.join(folder_path, "peakByMinute.csv")
write_record_peak(outputFile, peakByMinute, 'minute')

# write csv for record create peak per 0.1 second (process time)
processTimePeak = peakParser.calculateRecordCreateProcessTimePeak(recordArrByWs)
outputFile = os.path.join(folder_path, "peakByProcessTime.csv")
write_record_peak(outputFile, processTimePeak, 'process')

outputFile = os.path.join(folder_path, "recognizeTimeSummarize.csv")
write_recognize_time_summarize_file(outputFile, workstations, recognizeTime)
print("export summarize report, took " + str(round(time.time()-startTime, 2)) + " seconds")

# write csv for queryorinsert
outputFile = os.path.join(folder_path, "queryorinsertSummary.csv")
write_queryorinsert_summary_file(outputFile, workstations, queryorinsertListByWs)

