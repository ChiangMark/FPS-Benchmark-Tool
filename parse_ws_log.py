import os
import csv
import math
import sys
from collections import defaultdict

isPython2 = sys.version_info < (3, 0)
folder_path = ".\\logs"
file_attr = 'wb' if isPython2 else 'w'

SKIP_FRAME_DELAY = False
LOG_START_TIME = 0
LOG_END_TIME = 24

class parse_ws_log:
    def __init__(self, wsName, startTime, endTime):
        self.processors = [self.processType0, self.processType1, self.processType2, self.processType3, self.processType4, self.processType5]
        self.wsName = wsName
        self.extractDelayOutput = []        # type0
        self.queryDelayOutput = []          # type1
        self.createRecordDelayOutput = []   # type2
        self.frameDelayOutput = []          # type3
        self.usageOutput = {'CPU': [], 'GRAM': [], 'GUsage': []}    # type4
        self.fpsOutput = {'faceExtract': [], 'frameTimeFaceExtract': [], 'faceAdvExtract': [], 'frameTimeFaceAdvExtract': []}   # type5
        self.startTime = startTime
        self.endTime = endTime

    def parseType0(self, message):
        ret = None
        try:
            segments = message.split('.')
            segLen = len(segments)
            for i, segment in enumerate(segments):
                segments[i] = segment.strip(" ")
            segment0 = segments[0].split(" ")
            frameId = segment0[-1]
            cameraId = segment0[-3]
            recordId = segments[12].split(" ")[-1] if segLen==15 else ""
            delayTime = segment0[2]
            extractTime = segments[1].split(" ")[1]
            qcTime = segments[2].split(" ")[2]
            advExtract = segments[3].split(" ")[2]
            extractFaceImage = segments[4].split(" ")[3]
            compressWaitTime = segments[5].split(" ")[3]
            compressTime = segments[6].split(" ")[1]
            extractThumbTime = segments[7].split(" ")[2]
            waitAsNTime = segments[8].split(" ")[4]
            asNTime = segments[9].split(" ")[4]
            waitAsFTime = segments[10].split(" ")[4]
            asFTime = segments[11].split(" ")[4]
            createTime = segments[12].split(" ")[2] if segLen==14 else ""
            timeAfterPrevLogin = segments[13].split(" ")[2] if segLen==15 else ""

            # print("frameId: " + frameId)
            # print("cameraId: " + cameraId)
            # print("recordId: " + recordId)
            # print("delayTime: " + delayTime)
            # print("extractTime: " + extractTime)
            # print("qcTime: " + qcTime)
            # print("advExtract: " + advExtract)
            # print("extractFaceImage: " + extractFaceImage)
            # print("compressWaitTime: " + compressWaitTime)
            # print("compressTime: " + compressTime)
            # print("extractThumbTime: " + extractThumbTime)
            # print("waitAsNTime: " + waitAsNTime)
            # print("asNTime: " + asNTime)
            # print("waitAsFTime: " + waitAsFTime)
            # print("asFTime: " + asFTime)
            # print("timeAfterPrevLogin: " + timeAfterPrevLogin)
            ret = [self.wsName, frameId, cameraId, recordId, delayTime, extractTime, qcTime, advExtract, extractFaceImage, compressWaitTime, compressTime, extractThumbTime, waitAsNTime, asNTime, waitAsFTime, asFTime, createTime, timeAfterPrevLogin]
        except:
            pass
        return ret

    def processType0(self, infos):
        arr = [infos[2].split('.')[0]]        #time
        results = self.parseType0(infos[-1])
        if results:
            self.extractDelayOutput.append(arr + results)

    def parseType1(self, message, logTime):
        segments = message.split(" ")
        try:
            delayTime = int(segments[3])
            frameId = int(segments[9])
            itemsInQueue = int(segments[12])
            queryTime = int(segments[18])
            batch = int(segments[22].strip("."))

            return [logTime, self.wsName, frameId, delayTime, itemsInQueue, queryTime, batch]
        except:
            return None

    def processType1(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType1(infos[-1], logTime)
        if results:
            self.queryDelayOutput.append(results)

    def parseType2(self, message, logTime):
        ret = None
        try:
            segments = message.split(" ")
            if segments[6] != 'create':
                return ret
            frameId = int(segments[11][:-1]) #delete ending period
            recordId = int(segments[8])
            frameToQuery = int(segments[16])
            queryToUpload = int(segments[21])
            uploadTask = int(segments[25])
            itemToWait = int(segments[28])
            ret = [logTime, self.wsName, frameId, recordId, frameToQuery, queryToUpload, uploadTask, itemToWait]
        except:
            pass
        return ret


    def processType2(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType2(infos[-1], logTime)
        if results:
            self.createRecordDelayOutput.append(results)

    def parseType3(self, message):
        segments = message.split(" ")
        # type3: The frame 1421569 delay too long: 6411 for camera 15
        if segments[0] != 'The' or segments[1] != 'frame':
            return None
        return [segments[2], segments[6], segments[9]]

    def processType3(self, infos):
        result = self.parseType3(infos[-1])
        if result:
            frameId, delayTime, cameraId = result
            time = infos[2].split('.')[0]
            self.frameDelayOutput.append([self.wsName, int(cameraId), frameId, int(delayTime), time])

    def parseType4(self, message, logTime):
        segments = message.split(" ")
        result = None
        try:
            if segments[0] == 'CPU':
                # ex. CPU usage 21%, Memory usage 18294 MB of 261795 MB
                subType = 'CPU'
                usage = segments[2].split("%")[0]
                ramUsage = segments[5]
                ramTotal = segments[8]
                result = [self.wsName, logTime, subType, usage, ramUsage, ramTotal]
            elif segments[0] == 'GPU':
                if segments[2] == 'free':
                    # ex. GPU 0 free memory usage 35810.6 MB, total memory usage 49139.5 MB
                    subType = 'GRAM'
                    gid = segments[1]
                    usage = segments[5]
                    total = segments[10]
                    result = [self.wsName, logTime, subType, gid, usage, total]
                elif segments[2] == 'Usage':
                    # ex. GPU 0 Usage 8 %, Decoder usage 32
                    subType = 'GUsage'
                    gid = segments[1]
                    usage = segments[3]
                    total = segments[7]
                    result = [self.wsName, logTime, subType, gid, usage, total]
        except:
            return None

        return result

    def processType4(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType4(infos[-1], logTime)
        if results and results[2] in self.usageOutput.keys():
            self.usageOutput[results[2]].append(results)

    def parseType5(self, message, logTime):
        segments = message.split(" ")
        result = None
        try:
            if segments[2] == 'Face':
                if segments[3] == 'Extract':
                    # ex. GPU 0 Face Extract Rate: 11.257 fps. avg: 0.0158329 fps. peak: 11.257 fps.
                    subType = 'faceExtract'
                    gpuId = segments[1]
                    peak = segments[5]
                    avg = segments[8]
                    result = [self.wsName, logTime, subType, gpuId, peak, avg]
                elif segments[3] == 'Adv':
                    # ex. GPU 0 Frame Time Face Extract Rate: 12.7701 fps. avg: 0.0159036 fps. peak: 12.7701 fps.
                    subType = 'faceAdvExtract'
                    gpuId = segments[1]
                    peak = segments[6]
                    avg = segments[9]
                    result = [self.wsName, logTime, subType, gpuId, peak, avg]
            elif segments[2] == 'Frame':
                if segments[5] == 'Extract':
                    # ex. GPU 0 Face Adv Extract Rate: 4.64684 fps. avg: 0.0028447 fps. peak: 4.64684 fps.
                    subType = 'frameTimeFaceExtract'
                    gpuId = segments[1]
                    peak = segments[7]
                    avg = segments[10]
                    result = [self.wsName, logTime, subType, gpuId, peak, avg]
                elif segments[5] == 'Adv':
                    # ex. GPU 0 Frame Time Face Adv Extract Rate: 5.70342 fps. avg: 0.00291569 fps. peak: 5.70342 fps.
                    subType = 'frameTimeFaceAdvExtract'
                    gpuId = segments[1]
                    peak = segments[8]
                    avg = segments[11]
                    result = [self.wsName, logTime, subType, gpuId, peak, avg]
        except:
            return None

        return result

    def processType5(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType5(infos[-1], logTime)
        if results and results[2] in self.fpsOutput.keys():
            self.fpsOutput[results[2]].append(results)

    def getInfoType(self, infos):
        ## example
        # type0: Delay time 1359 ms is too long to update login record for camera 69 frame 1320500. Extract: 497 ms. Quality check: 1 ms. Adv extract: 64 ms. Extract face image: 7 ms. Compress wait time: 0 ms. Compress: 3 ms. Extract thumb: 0 ms. Wait anti-spoofing N time: 0 ms. Anti spoofing N time: 0 ms. Wait anti-spoofing F time: 0 ms. Anti spoofing F time: 0 ms. After 0 ms to upload record 1260411. There is 862 ms after previous GetLoginFace.
        # type1: Query delay time 332 ms too long for frame 55355 after wait 0 items in queue. Process query 256 ms in batch 1
        # type2: Delay time is too long for create record 5273 for frame 23648. Frame time to query: 763 ms. Query to upload: 1375 ms, upload task: 41 ms, wait 1 items to upload.
        # type3: The frame 1421569 delay too long: 6411 for camera 15
        # type4: CPU usage 21%, Memory usage 18294 MB of 261795 MB
        #        GPU 0 free memory usage 35810.6 MB, total memory usage 49139.5 MB
        #        GPU 0 Usage 8 %, Decoder usage 32
        # type5: GPU 0 Face Extract Rate: 11.257 fps. avg: 0.0158329 fps. peak: 11.257 fps.
        #        GPU 0 Frame Time Face Extract Rate: 12.7701 fps. avg: 0.0159036 fps. peak: 12.7701 fps.
        #        GPU 0 Face Adv Extract Rate: 4.64684 fps. avg: 0.0028447 fps. peak: 4.64684 fps.
        #        GPU 0 Frame Time Face Adv Extract Rate: 5.70342 fps. avg: 0.00291569 fps. peak: 5.70342 fps.

        warningTypeDict = {
            "FaceProcessSequenceStatus::GetLoginFace": '0',
            "DataUploader::DoQueryData": '1',
            "DataUploader::DoUploadData": '2',
            "CameraInfo::OnVideoFrame": '3',
            "FRManager::Proc_ResourceMonitor": '4',
            "ProcessInfo::OnFaceExtractCompleted": '5'
        }
        if infos[1] == "warning" or infos[1] == "info":
            return warningTypeDict[infos[5]] if infos[5] in warningTypeDict else None

    def checkTime(self, logTime):
        try:
            hour = int(logTime.split(':')[0])
        except:
            return False
        return True if (hour>=self.startTime and hour<self.endTime) else False

    def parseLine(self, line):
        infos = line.split("]")
        for i, info in enumerate(infos):
            infos[i] = info.strip(" [")
        if len(infos)<7:
            return
        if not self.checkTime(infos[2]):
            return
        infoType = self.getInfoType(infos)
        if infoType:
            self.processors[int(infoType)](infos)

    def parseFile(self, filepath):
        with open(filepath, 'r') as f:
            for line in f.readlines():
                self.parseLine(line)



def write_extract_delay_file(filename, outputTopFile, wsDelayOutputs):
    print(len(wsDelayOutputs))
    maxDelayData = dict()
    maxDelaySize = int(math.ceil(len(wsDelayOutputs)*0.1))
    avg = [0] * 7
    maximum = [0] * 7
    sec1 = [0] * 7
    sec2 = [0] * 7
    sec3 = [0] * 7
    sec5 = [0] * 7
    sec10 = [0] * 7
    sec60 = [0] * 7
    for output in wsDelayOutputs:
        delaytime = int(output[5])
        if (delaytime not in maxDelayData):
            maxDelayData[delaytime] = []
        maxDelayData[delaytime].append(int(output[2]))
        sorted_dict = dict(sorted(maxDelayData.items(), reverse=True))
        if (len(sorted_dict) < maxDelaySize):
            maxDelayData = sorted_dict
        else:
            maxDelayData = dict(list(sorted_dict.items())[:maxDelaySize])
        for i in range(5, 12):
            avg[i-5] += int(output[i])
            maximum[i-5] = max(maximum[i-5], int(output[i]))
            if int(output[i])>1000:
                sec1[i-5]+=1
            if int(output[i])>2000:
                sec2[i-5]+=1
            if int(output[i])>3000:
                sec3[i-5]+=1
            if int(output[i])>5000:
                sec5[i-5]+=1
            if int(output[i])>10000:
                sec10[i-5]+=1
            if int(output[i])>60000:
                sec60[i-5]+=1
    header = ['time', 'ws name', 'frameId', 'cameraId', 'recordId', 'extract delay',
          'extractTime', 'qcTime', 'advExtract', 'extractFaceImage', 'compressWaitTime',
          'compressTime', 'extractThumbTime', 'waitAsNTime', 'asNTime', 'waitAsFTime',
          'asFTime', 'create', 'upload record after previous']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        csv_writer.writerow(["avg", "", "", "", ""] + [round(float(number)/float(len(wsDelayOutputs)), 2) for number in avg])
        csv_writer.writerow(["max", "", "", "", ""] + maximum)
        csv_writer.writerow([">1s", "", "", "", ""] + sec1)
        csv_writer.writerow([">2s", "", "", "", ""] + sec2)
        csv_writer.writerow([">3s", "", "", "", ""] + sec3)
        csv_writer.writerow([">5s", "", "", "", ""] + sec5)
        csv_writer.writerow([">10s", "", "", "", ""] + sec10)
        csv_writer.writerow([">60s", "", "", "", ""] + sec60)
        
        csv_writer.writerows(wsDelayOutputs)

    with open(outputTopFile, file_attr) as csvTopfile:
        csvTopfile.write("Top 1% ({}):\n".format(maxDelaySize))
        csvTopfile.write("extract delay, frameId list\n")
        for key, value in sorted(maxDelayData.items(), reverse=True):
            csv_writer = csv.writer(csvTopfile)
            csv_writer.writerow([key] + value)


def write_extract_summarize_file(filename, wsDelayOutputs):
    dictByWS = defaultdict(lambda: [0, 0, 0, 0, 0, 0])    #[total count, extract delay, extract time, qc time, adv extract, extract face]
    maximum = [0] * 5
    ms100 = [0] * 5
    ms500 = [0] * 5
    sec1 = [0] * 5
    sec10 = [0] * 5
    exceed10 = [0] * 5
    for output in wsDelayOutputs:
        wsName = output[1]
        dictByWS[wsName][0] += 1
        dictByWS[wsName][1] += int(output[5])
        dictByWS[wsName][2] += int(output[6])
        dictByWS[wsName][3] += int(output[7])
        dictByWS[wsName][4] += int(output[8])
        dictByWS[wsName][5] += int(output[9])

        for i in range(5, 10):
            maximum[i-5] = max(maximum[i-5], int(output[i]))
            if int(output[i])<100:
                ms100[i-5] += 1
            elif int(output[i])<500:
                ms500[i-5] += 1
            elif int(output[i])<1000:
                sec1[i-5] += 1
            elif int(output[i])<10000:
                sec10[i-5] += 1
            else:
                exceed10[i-5] += 1


    header = ['', 'ws', 'Amount', 'T06 - T01 (ms)', 'Detect + VH(ms)', 'QC (ms)', 'UH (ms)', 'Extract Face Image (ms)']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        amount, t01, detect, qc, uh, extract = 0, 0, 0, 0, 0, 0
        for ws, value in sorted(dictByWS.items()):
            amount += value[0]
            t01 += value[1]
            detect += value[2]
            qc += value[3]
            uh += value[4]
            extract += value[5]
            csv_writer.writerow(["", ws, value[0]] + [round(float(number)/float(value[0]), 2) for number in value[1:]])
        csv_writer.writerow(["", 'Total', amount, round(float(t01)/float(amount), 2), round(float(detect)/float(amount), 2), round(float(qc)/float(amount), 2),
                             round(float(uh)/float(amount), 2), round(float(extract)/float(amount), 2)])

        csv_writer.writerow([""]*2 + ["max"] + maximum)
        csv_writer.writerow([""]*2 + ["<100ms"] + ms100)
        csv_writer.writerow([""]*2 + ["100~500ms"] + ms500)
        csv_writer.writerow([""]*2 + ["500ms~1s"] + sec1)
        csv_writer.writerow([""]*2 + ["1s~10s"] + sec10)
        csv_writer.writerow([""]*2 + [">10s"] + exceed10)
        
        count = len(wsDelayOutputs)
        csv_writer.writerow([""]*2 + ["<100ms %"] + [round(float(number) / float(count), 3) for number in ms100])
        csv_writer.writerow([""]*2 + ["100~500ms %"] + [round(float(number) / float(count), 3) for number in ms500])
        csv_writer.writerow([""]*2 + ["500ms~1s %"] + [round(float(number) / float(count), 3) for number in sec1])
        csv_writer.writerow([""]*2 + ["1s~10s %"] + [round(float(number) / float(count), 3) for number in sec10])
        csv_writer.writerow([""]*2 + [">10s %"] + [round(float(number) / float(count), 3) for number in exceed10])

def write_frame_delay_file(filename, outputTopFile, wsDelayOutputs):
    # print(len(wsDelayOutputs))
    maxDelayData = dict()
    maxDelaySize = int(math.ceil(len(wsDelayOutputs) * 0.1))
    dictByWS = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0, 0]))
    for output in wsDelayOutputs:
        #output: [wsName, cameraId, frameId, delayTime, logTime]
        wsName, cameraId, frameId, delayTime, logTime = output

        #find top
        if (delayTime not in maxDelayData):
            maxDelayData[delayTime] = []
        maxDelayData[delayTime].append(int(frameId))
        sorted_dict = dict(sorted(maxDelayData.items(), reverse=True))
        if (len(sorted_dict) < maxDelaySize):
            maxDelayData = sorted_dict
        else:
            maxDelayData = dict(list(sorted_dict.items())[:maxDelaySize])

        dictByWS[wsName][cameraId][0] += delayTime
        dictByWS[wsName][cameraId][1] += 1
        if delayTime>1000:
            dictByWS[wsName][cameraId][2] += 1
        if delayTime>2000:
            dictByWS[wsName][cameraId][3] += 1
        if delayTime>3000:
            dictByWS[wsName][cameraId][4] += 1
        if delayTime>5000:
            dictByWS[wsName][cameraId][5] += 1
        if delayTime>10000:
            dictByWS[wsName][cameraId][6] += 1
        if delayTime>60000:
            dictByWS[wsName][cameraId][7] += 1
    # print(dictByWS)
    header = ['ws name', 'cameraId', 'avg delay time', 'count', '>1s', '>2s', '>3s', '>5s', '>10s', '>60s']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        for ws, cameraId in sorted(dictByWS.items()):
            for camId, data in sorted(dictByWS[ws].items()):
                csv_writer.writerow([ws, camId, round(float(data[0])/float(data[1]), 2), data[1], data[2], data[3], data[4], data[5], data[6], data[7]])

    with open(outputTopFile, file_attr) as csvTopfile:
        csvTopfile.write("Top 1% ({}):\n".format(maxDelaySize))
        csvTopfile.write("frame delay, frameId list\n")
        for key, value in sorted(maxDelayData.items(), reverse=True):
            csv_writer = csv.writer(csvTopfile)
            csv_writer.writerow([key] + value)


def write_frame_summarize_file(filename, wsDelayOutputs):
    dictByWS = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0, 0])    #[sum, max, >1s, >2s, >3s, >5s, >10s, >60s]
    totalTime = 0
    totalMax = 0
    totalCount = 0
    totalDelayIntervalDict = {'1s': 0, '2s': 0, '3s': 0, '5s': 0, '10s': 0, '60s': 0}
    for output in wsDelayOutputs:
        wsName, _, _, delayTime, _ = output
        dictByWS[wsName][0] += int(delayTime)
        totalTime += int(delayTime)
        totalCount += 1
        dictByWS[wsName][1] = max(dictByWS[wsName][1], int(delayTime))
        totalMax = max(totalMax, int(delayTime))
        if delayTime>1000:
            dictByWS[wsName][2] += 1
            totalDelayIntervalDict['1s'] += 1
        if delayTime>2000:
            dictByWS[wsName][3] += 1
            totalDelayIntervalDict['2s'] += 1
        if delayTime>3000:
            dictByWS[wsName][4] += 1
            totalDelayIntervalDict['3s'] += 1
        if delayTime>5000:
            dictByWS[wsName][5] += 1
            totalDelayIntervalDict['5s'] += 1
        if delayTime>10000:
            dictByWS[wsName][6] += 1
            totalDelayIntervalDict['10s'] += 1
        if delayTime>60000:
            dictByWS[wsName][7] += 1
            totalDelayIntervalDict['60s'] += 1
    
    header = ['ws', 'Amount', 'Avg. (ms)', 'Max. (ms)', 'Frame pool size Max.', '>2s', '>3s', '>5s', '>10s', '>60s']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        for ws, values in sorted(dictByWS.items()):
            csv_writer.writerow([ws, values[2], round(float(values[0])/float(values[2]), 2), values[1], '', values[3], values[4], values[5], values[6], values[7]])
        csv_writer.writerow(['Total', totalDelayIntervalDict['1s'], round(float(totalTime) / float(totalCount), 2), totalMax, '', totalDelayIntervalDict['2s'], totalDelayIntervalDict['3s'], totalDelayIntervalDict['5s'], totalDelayIntervalDict['10s'], totalDelayIntervalDict['60s']])

def write_query_delay_file(filename, outputTopFile, wsDelayOutputs):
    maxDelayData = dict()
    maxDelaySize = int(math.ceil(len(wsDelayOutputs)*0.1))
    avg = [0] * 2
    maximum = [0] * 2
    sec1 = [0] * 2
    sec2 = [0] * 2
    sec3 = [0] * 2
    sec5 = [0] * 2
    sec10 = [0] * 2
    sec60 = [0] * 2
    for output in wsDelayOutputs:
        delayTime, queryTime = output[3], output[5]
        if (delayTime not in maxDelayData):
            maxDelayData[delayTime] = []
        maxDelayData[delayTime].append(int(output[2]))
        sorted_dict = dict(sorted(maxDelayData.items(), reverse=True))
        if (len(sorted_dict) < maxDelaySize):
            maxDelayData = sorted_dict
        else:
            maxDelayData = dict(list(sorted_dict.items())[:maxDelaySize])
        avg[0] += delayTime
        maximum[0] = max(maximum[0], delayTime)
        if delayTime>1000:
            sec1[0]+=1
        if delayTime>2000:
            sec2[0]+=1
        if delayTime>3000:
            sec3[0]+=1
        if delayTime>5000:
            sec5[0]+=1
        if delayTime>10000:
            sec10[0]+=1
        if delayTime>60000:
            sec60[0]+=1

        avg[1] += queryTime
        maximum[1] = max(maximum[1], queryTime)
        if queryTime>1000:
            sec1[1]+=1
        if queryTime>2000:
            sec2[1]+=1
        if queryTime>3000:
            sec3[1]+=1
        if queryTime>5000:
            sec5[1]+=1
        if queryTime>10000:
            sec10[1]+=1
        if queryTime>60000:
            sec60[1]+=1
        

    header = ['time', 'ws name', 'frameId', 'delay time', 'items in queue', 'query time', 'batch']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        csv_writer.writerow(["avg", "", "", round(float(avg[0])/float(len(wsDelayOutputs)), 2), "", round(float(avg[1])/float(len(wsDelayOutputs)), 2)])
        csv_writer.writerow(["max", "", "", maximum[0], "", maximum[1]])
        csv_writer.writerow([">1s", "", "", sec1[0], "", sec1[1]])
        csv_writer.writerow([">2s", "", "", sec2[0], "", sec2[1]])
        csv_writer.writerow([">3s", "", "", sec3[0], "", sec3[1]])
        csv_writer.writerow([">5s", "", "", sec5[0], "", sec5[1]])
        csv_writer.writerow([">10s", "", "", sec10[0], "", sec10[1]])
        csv_writer.writerow([">60s", "", "", sec60[0], "", sec60[1]])
        
        for output in wsDelayOutputs:
            csv_writer.writerow(output)

    with open(outputTopFile, file_attr) as csvTopfile:
        csvTopfile.write("Top 1% ({}):\n".format(maxDelaySize))
        csvTopfile.write("query delay, frameId list\n")
        for key, value in sorted(maxDelayData.items(), reverse=True):
            csv_writer = csv.writer(csvTopfile)
            csv_writer.writerow([key] + value)


def write_query_summarize_file(filename, wsDelayOutputs):
    dictByWS = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0])    #[total count, delay time, delay time max, query time, query time max, batch size, batch size max]
    maximum = [0] * 3
    exceed500 = 0
    exceed1000 = 0
    totalSum = [0] * 3
    totalCount = 0
    for output in wsDelayOutputs:
        wsName = output[1]
        dictByWS[wsName][0] += 1
        totalCount += 1
        dictByWS[wsName][1] += output[3]
        totalSum[0] += output[3]
        dictByWS[wsName][2] = max(dictByWS[wsName][2], output[3])
        dictByWS[wsName][3] += output[5]
        totalSum[1] += output[5]
        dictByWS[wsName][4] = max(dictByWS[wsName][4], output[5])
        dictByWS[wsName][5] += output[6]
        totalSum[2] += output[6]
        dictByWS[wsName][6] = max(dictByWS[wsName][6], output[6])

        # delay time
        maximum[0] = max(maximum[0], output[3])
        if output[3] > 500:
            exceed500 += 1
        if output[3] > 1000:
            exceed1000 += 1

        # query time
        maximum[1] = max(maximum[1], output[5])
        if output[5] > 500:
            exceed500 += 1
        if output[5] > 1000:
            exceed1000 += 1

        # batch size
        maximum[2] = max(maximum[2], output[6])

    header = ['', 'Central', 'Amount', 'Avg. (ms)', 'Max. (ms)', '>0.5s', '>1s', 'Query time avg. (ms)', 'Query time max. (ms)', 'Batch size avg.', 'Batch size max.']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        for ws, value in sorted(dictByWS.items()):
            row_data = ["", ws, value[0]]
            row_data += [round(float(value[1]) / float(value[0]), 2)]
            row_data += [value[2]]
            row_data += ['', '']
            row_data += [round(float(value[3]) / float(value[0]), 2)]
            row_data += [value[4]]
            row_data += [round(float(value[5]) / float(value[0]), 2)]
            row_data += [value[6]]
            csv_writer.writerow(row_data)

        total_row_data = ["", 'Total', totalCount]
        total_row_data += [round(float(totalSum[0]) / float(totalCount), 2)]
        total_row_data += [maximum[0]]
        total_row_data += [str(exceed500)]
        total_row_data += [str(exceed1000)]
        total_row_data += [round(float(totalSum[1]) / float(totalCount), 2)]
        total_row_data += [maximum[1]]
        total_row_data += [round(float(totalSum[2]) / float(totalCount), 2)]
        total_row_data += [maximum[2]]
        csv_writer.writerow(total_row_data)
        
        # csv_writer.writerow([])

        # csv_writer.writerow([""]*3 + [">0.5s"] + [str(exceed500)])
        # csv_writer.writerow([""]*3 + [">1s"] + [str(exceed1000)])

        # csv_writer.writerow([])
        
        # count = len(wsDelayOutputs)
        # csv_writer.writerow([""]*3 + [">0.5s %"] + [str(round(float(exceed500) / float(count), 3))])
        # csv_writer.writerow([""]*3 + [">1s %"] + [str(round(float(exceed500) / float(count), 3))])


def write_create_record_delay_file(filename, outputQueryTopFile, outputUploadTopFile, wsDelayOutputs):
    maxDelayForQueryData = dict()
    maxDelayForUploadData = dict()
    maxDelayForQuerySize = int(math.ceil(len(wsDelayOutputs)*0.1))
    maxDelayForUploadSize = int(math.ceil(len(wsDelayOutputs)*0.1))
    avg = [0] * 3
    maximum = [0] * 3
    sec1 = [0] * 3
    sec2 = [0] * 3
    sec3 = [0] * 3
    sec5 = [0] * 3
    sec10 = [0] * 3
    sec60 = [0] * 3
    for output in wsDelayOutputs:
        querytime = int(output[4])
        uploadtime = int(output[5])
        # find top for query frame
        if (querytime not in maxDelayForQueryData):
            maxDelayForQueryData[querytime] = []
        maxDelayForQueryData[querytime].append(int(output[2]))
        sorted_dict = dict(sorted(maxDelayForQueryData.items(), reverse=True))
        if (len(sorted_dict) < maxDelayForQuerySize):
            maxDelayForQueryData = sorted_dict
        else:
            maxDelayForQueryData = dict(list(sorted_dict.items())[:maxDelayForQuerySize])

        # find top for upload frame
        try:
            if (uploadtime not in maxDelayForUploadData):
                maxDelayForUploadData[uploadtime] = []
            maxDelayForUploadData[uploadtime].append(int(output[2]))
            sorted_dict = dict(sorted(maxDelayForUploadData.items(), reverse=True))
            if (len(sorted_dict) < maxDelayForUploadSize):
                maxDelayForUploadData = sorted_dict
            else:
                maxDelayForUploadData = dict(list(sorted_dict.items())[:maxDelayForUploadSize])
        except Exception as e:
            print(e)

        for i in range(4, 7):
            #
            avg[i-4] += output[i]
            maximum[i-4] = max(maximum[i-4], output[i])
            if output[i]>1000:
                sec1[i-4]+=1
            if output[i]>2000:
                sec2[i-4]+=1
            if output[i]>3000:
                sec3[i-4]+=1
            if output[i]>5000:
                sec5[i-4]+=1
            if output[i]>10000:
                sec10[i-4]+=1
            if output[i]>60000:
                sec60[i-4]+=1        

    header = ['time', 'ws name', 'frameId', 'recordId', 'frame to query', 'query to upload', 'upload', 'items to wait']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)

        csv_writer.writerow(["avg", "", "", ""] + [round(float(number)/float(len(wsDelayOutputs)), 2) for number in avg])
        csv_writer.writerow(["max", "", "", "", maximum[0], maximum[1], maximum[2]])
        csv_writer.writerow([">1s", "", "", "", sec1[0], sec1[1], sec1[2]])
        csv_writer.writerow([">2s", "", "", "", sec2[0], sec2[1], sec2[2]])
        csv_writer.writerow([">3s", "", "", "", sec3[0], sec3[1], sec3[2]])
        csv_writer.writerow([">5s", "", "", "", sec5[0], sec5[1], sec5[2]])
        csv_writer.writerow([">10s", "", "", "", sec10[0], sec10[1], sec10[2]])
        csv_writer.writerow([">60s", "", "", "", sec60[0], sec60[1], sec60[2]])
        
        for output in wsDelayOutputs:
            csv_writer.writerow(output)

    with open(outputQueryTopFile, file_attr) as csvTopfile:
        csvTopfile.write("QueryFrame Top 1% ({}):\n".format(maxDelayForQuerySize))
        csvTopfile.write("frame to query, frameId list\n")
        for key, value in sorted(maxDelayForQueryData.items(), reverse=True):
            csv_writer = csv.writer(csvTopfile)
            csv_writer.writerow([key] + value)

    with open(outputUploadTopFile, file_attr) as csvTopfile:
        csvTopfile.write("\n\nUploadFrame Top 1% ({}):\n".format(maxDelayForUploadSize))
        csvTopfile.write("frame to upload, frameId list\n")
        for key, value in sorted(maxDelayForUploadData.items(), reverse=True):
            csv_writer = csv.writer(csvTopfile)
            csv_writer.writerow([key] + value)


def write_create_record_summarize_file(filename, wsDelayOutputs):
    dictByWS = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0])    #[total count, frame to query, frame to query max, query to upload, query to upload max, upload, upload max]
    maximum = [0] * 3
    ms100 = [0] * 3
    ms500 = [0] * 3
    sec1 = [0] * 3
    exceed1 = [0] * 3
    totalSum = [0] * 3
    totalCount = 0
    for output in wsDelayOutputs:
        wsName = output[1]
        dictByWS[wsName][0] += 1
        totalCount += 1
        dictByWS[wsName][1] += output[4]
        totalSum[0] += output[4]
        dictByWS[wsName][2] = max(dictByWS[wsName][2], output[4])
        dictByWS[wsName][3] += output[5]
        totalSum[1] += output[5]
        dictByWS[wsName][4] = max(dictByWS[wsName][4], output[5])
        dictByWS[wsName][5] += output[6]
        totalSum[2] += output[6]
        dictByWS[wsName][6] = max(dictByWS[wsName][6], output[6])

        # delay time
        for i in range(4, 7):
            maximum[i-4] = max(maximum[i-4], output[i])
            if output[i]<100:
                ms100[i-4] += 1
            elif output[i]<500:
                ms500[i-4] += 1
            elif output[i]<1000:
                sec1[i-4] += 1
            else:
                exceed1[i-4] += 1

    header = ['', 'Central', 'Amount', 'Frame to Query avg. (ms)', 'Frame to Query max. (ms)', 'Query to upload avg. (ms)', 'Query to upload max. (ms)', 
              'Upload avg.', 'Upload max']
    with open(filename, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(header)
        
        for ws, value in sorted(dictByWS.items()):
            row_data = ["", ws, value[0]]
            row_data += [round(float(value[1])/float(value[0]), 2)]
            row_data += [value[2]]
            row_data += [round(float(value[3]) / float(value[0]), 2)]
            row_data += [value[4]]
            row_data += [round(float(value[5]) / float(value[0]), 2)]
            row_data += [value[6]]
            csv_writer.writerow(row_data)

        total_row_data = ["", 'Total', totalCount]
        total_row_data += [round(float(totalSum[0]) / float(totalCount), 2)]
        total_row_data += [maximum[0]]
        total_row_data += [round(float(totalSum[1]) / float(totalCount), 2)]
        total_row_data += [maximum[1]]
        total_row_data += [round(float(totalSum[2]) / float(totalCount), 2)]
        total_row_data += [maximum[2]]
        csv_writer.writerow(total_row_data)

        csv_writer.writerow([])

        csv_writer.writerow([""]*3 + ["<100ms"] + ms100)
        csv_writer.writerow([""]*3 + ["100~500ms"] + ms500)
        csv_writer.writerow([""]*3 + ["500ms~1s"] + sec1)
        csv_writer.writerow([""]*3 + [">1s"] + exceed1)
        csv_writer.writerow([])
        
        count = len(wsDelayOutputs)
        csv_writer.writerow([""]*3 + ["<100ms %"] + [round(float(number) / float(count), 3) for number in ms100])
        csv_writer.writerow([""]*3 + ["100~500ms %"] + [round(float(number) / float(count), 3) for number in ms500])
        csv_writer.writerow([""]*3 + ["500ms~1s %"] + [round(float(number) / float(count), 3) for number in sec1])
        csv_writer.writerow([""]*3 + [">1s"] + [round(float(number) / float(count), 3) for number in exceed1])


def write_usage_file(cpuUsageFile, gpuMemFile, gpuUsageFile, usageOutputs):
    # 'CPU': result = [self.wsName, logTime, subType, usage, ramUsage, ramTotal]
    # 'GRAM': result = [self.wsName, logTime, subType, gid, usage, total]
    # 'GUsage': result = [self.wsName, logTime, subType, gid, usage, total]

    with open(cpuUsageFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        header = ['time', 'usage', 'ramUsage', 'ramTotal']
        csv_writer.writerow(header)
        for result in usageOutputs['CPU']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])
    with open(gpuMemFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        header = ['time', 'gid', 'usage', 'total']
        csv_writer.writerow(header)
        for result in usageOutputs['GRAM']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])
    with open(gpuUsageFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        header = ['time', 'gid', 'usage', 'total']
        csv_writer.writerow(header)
        for result in usageOutputs['GUsage']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])


def write_fps_file(fpsFaceExtractFile, fpsFrameTimeFaceExtractFile, fpsFaceAdvExtractFile, fpsFrameTimeFaceAdvExtractFile, fpsOutputs):
    # [self.wsName, logTime, subType, gpuId, peak, avg]
    header = ['time', 'gpuId', 'peak', 'avg']
    with open(fpsFaceExtractFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        for result in fpsOutputs['faceExtract']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])
    with open(fpsFrameTimeFaceExtractFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        for result in fpsOutputs['frameTimeFaceExtract']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])
    with open(fpsFaceAdvExtractFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        for result in fpsOutputs['faceAdvExtract']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])
    with open(fpsFrameTimeFaceAdvExtractFile, file_attr) as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(header)
        for result in fpsOutputs['frameTimeFaceAdvExtract']:
            csv_writer.writerow([result[1], result[3], result[4], result[5]])


## for single file testing
# wsDelayOutput = []
# frameDelayOutput = []
# queryDelayOutput = []
# parser = parse_ws_log("ws")
# parser.parseFile("WS20231026\\ws\\Workstation_2023-10-26_18-48.log")
# print(parser.extractDelayOutput)
# print(parser.delayFrameDict)
# wsDelayOutput = wsDelayOutput + parser.extractDelayOutput
# frameDelayOutput = frameDelayOutput + parser.frameDelayOutput
# queryDelayOutput = queryDelayOutput + parser.queryDelayOutput
# write_extract_delay_file("extractOutput.csv", wsDelayOutput)
# write_frame_delay_file("frame.csv", frameDelayOutput)
# write_query_delay_file("queryOutput.csv", queryDelayOutput)



def find_log_files(folder_path):
    log_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".log") and "Workstation" in file:
                log_files.append(os.path.join(root, file))
    return log_files

wsDelayOutput = []
frameDelayOutput = []
workstations = []
for entry in os.listdir(folder_path):
    if os.path.isdir(os.path.join(folder_path, entry)):
        workstations.append(entry)
print(workstations)

extractDelayTotalList = []
queryDelayOutput = []
createRecordDelayOutput = []
frameDelayList = []
for workstation in workstations:
    log_files = find_log_files(os.path.join(folder_path, workstation))
    print(log_files)
    extractDelayList = []
    usageOutput = {'CPU': [], 'GRAM': [], 'GUsage': []}
    fpsOutput = {'faceExtract': [], 'frameTimeFaceExtract': [], 'faceAdvExtract': [], 'frameTimeFaceAdvExtract': []}

    for log_file in log_files:
        print("processing file " + log_file)
        parser = parse_ws_log(workstation, LOG_START_TIME, LOG_END_TIME)
        parser.parseFile(log_file)
        # print("extract delay count: " + str(len(parser.extractDelayOutput)))
        # print("frame delay count: " + str(len(parser.frameDelayOutput)))
        extractDelayList = extractDelayList + parser.extractDelayOutput
        extractDelayTotalList = extractDelayTotalList + parser.extractDelayOutput
        queryDelayOutput = queryDelayOutput + parser.queryDelayOutput
        createRecordDelayOutput = createRecordDelayOutput + parser.createRecordDelayOutput
        if not SKIP_FRAME_DELAY:
            frameDelayList = frameDelayList + parser.frameDelayOutput
        for key in usageOutput.keys():
            usageOutput[key] = usageOutput[key] + parser.usageOutput[key]
        for key in fpsOutput.keys():
            fpsOutput[key] = fpsOutput[key] + parser.fpsOutput[key]
    outputFile = os.path.join(folder_path, workstation + '.csv')
    outputTopFile = os.path.join(folder_path, workstation + '_extractDelay_top.csv')
    outputCpuUsageFile = os.path.join(folder_path, workstation + '_CPU_usage.csv')
    outputGpuMemFile = os.path.join(folder_path, workstation + '_GPU_memory.csv')
    outputGpuUsageFile = os.path.join(folder_path, workstation + '_GPU_usage.csv')
    outputFpsFaceExtractFile = os.path.join(folder_path, workstation + '_fps_faceExtract.csv')
    outputFpsFrameTimeFaceExtractFile = os.path.join(folder_path, workstation + '_fps_frameTimeFaceExtract.csv')
    outputFpsFaceAdvExtractFile = os.path.join(folder_path, workstation + '_fps_faceAdvExtract.csv')
    outputFpsFrameTimeFaceAdvExtractFile = os.path.join(folder_path, workstation + '_fps_frameTimeFaceAdvExtract.csv')
    # write csv for each workstation
    if extractDelayList:
        write_extract_delay_file(outputFile, outputTopFile, extractDelayList)
    write_usage_file(outputCpuUsageFile, outputGpuMemFile, outputGpuUsageFile, usageOutput)
    write_fps_file(outputFpsFaceExtractFile, outputFpsFrameTimeFaceExtractFile, outputFpsFaceAdvExtractFile, outputFpsFrameTimeFaceAdvExtractFile, fpsOutput)

# write csv for frame delay
if not SKIP_FRAME_DELAY:
    outputFile = os.path.join(folder_path, "frameDelay.csv")
    outputTopFile = os.path.join(folder_path, "frameDelay_top")
    outputTopFile += ".csv"
    if frameDelayList:
        write_frame_delay_file(outputFile, outputTopFile, frameDelayList)

# write csv for extract delay summary
outputFile = os.path.join(folder_path, "extractSummarize.csv")
if extractDelayTotalList:
    write_extract_summarize_file(outputFile, extractDelayTotalList) 

# write csv for frame delay summary
if not SKIP_FRAME_DELAY:
    outputFile = os.path.join(folder_path, "frameSummarize.csv")
    if frameDelayList:
        write_frame_summarize_file(outputFile, frameDelayList)

# write csv for query delay
outputFile = os.path.join(folder_path, "queryOutput.csv")
outputTopFile = os.path.join(folder_path, "queryOutput_top")
outputTopFile += ".csv"
if queryDelayOutput:
    write_query_delay_file(outputFile, outputTopFile, queryDelayOutput)

# write csv for query delay summary
outputFile = os.path.join(folder_path, "querySummarize.csv")
if queryDelayOutput:
    write_query_summarize_file(outputFile, queryDelayOutput)

# write csv for create record delay
outputFile = os.path.join(folder_path, "createRecordOutput.csv")
outputQueryTopFile = os.path.join(folder_path, "createRecordQueryTimeOutput_top")
outputQueryTopFile += ".csv"
outputUploadTopFile = os.path.join(folder_path, "createRecordUploadTimeOutput_top")
outputUploadTopFile += ".csv"
if createRecordDelayOutput:
    write_create_record_delay_file(outputFile, outputQueryTopFile, outputUploadTopFile, createRecordDelayOutput)

# write csv for create record delay
outputFile = os.path.join(folder_path, "createRecordSummarize.csv")
if createRecordDelayOutput:
    write_create_record_summarize_file(outputFile, createRecordDelayOutput)




