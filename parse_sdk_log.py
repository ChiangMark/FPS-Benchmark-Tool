import os
import csv
from collections import defaultdict

folder_path = ".\\logs"

LOG_START_TIME = 0
LOG_END_TIME = 24


class parse_ws_log:
    def __init__(self, wsName, startTime, endTime):
        self.processors = [self.processType0, self.processType1, self.processType2, self.processType3]
        self.wsName = wsName
        self.sdkFrameBufferSizeOutput = []  # type0
        self.sdkFramePoolSizeOutput = []  # type1
        self.sdkQueueSizeOutput = []  # type1
        self.sdkQCQueueSizeOutput = []  # type1
        self.sdkEventTrackCallbackOutput = []  # type2
        self.sdkTrackCallbackOutput = []  # type3
        self.startTime = startTime
        self.endTime = endTime

    def parseType0(self, message, logTime):
        segments = message.split(" ")
        try:
            bufferSize = int(segments[5])
        except:
            try:
                bufferSize = int(segments[8])
            except:
                return None

        return [self.wsName, logTime, bufferSize]

    def processType0(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType0(infos[-1], logTime)
        if results:
            self.sdkFrameBufferSizeOutput.append(results)

    def parseType1(self, message, logTime):
        segments = message.split(" ")
        result = None, None
        try:
            if segments[2] == 'frame':
                # GPU 0 frame pool status available frame 14/420 for camera count 15
                gpuId = segments[1]
                frameRatio = segments[7]
                frameRatioListTemp = frameRatio.split("/")
                frameCount = frameRatioListTemp[0]
                frameTotal = frameRatioListTemp[1]
                cameraCount = segments[11]
                result = 'frame', [self.wsName, logTime, gpuId, frameCount, frameTotal, cameraCount]
            elif 'Queue' in segments[2]:
                # GPU 0 Queue: Available:(27), Queued:(1), Processing:(100), Detect:(3), Extract:(97).
                gpuId = segments[1]
                availableTemp = segments[3]
                available = availableTemp.split('(')[1].split(')')[0]
                queuedTemp = segments[4]
                queued = queuedTemp.split('(')[1].split(')')[0]
                processingTemp = segments[5]
                processing = processingTemp.split('(')[1].split(')')[0]
                detectTemp = segments[6]
                detect = detectTemp.split('(')[1].split(')')[0]
                extractTemp = segments[7]
                extract = extractTemp.split('(')[1].split(')')[0]
                result = 'Queue', [self.wsName, logTime, gpuId, available, queued, processing, detect, extract]
            elif 'Extract' in segments[2]:
                # GPU 0 Extract:(97), Occlusion(0), Id:(0), Pose:(0), Attr:(0), MaskId:(0).
                gpuId = segments[1]
                extractTemp = segments[2]
                extract = extractTemp.split('(')[1].split(')')[0]
                occlusionTemp = segments[3]
                occlusion = occlusionTemp.split('(')[1].split(')')[0]
                idTemp = segments[4]
                id = idTemp.split('(')[1].split(')')[0]
                poseTemp = segments[5]
                pose = poseTemp.split('(')[1].split(')')[0]
                attrTemp = segments[6]
                attr = attrTemp.split('(')[1].split(')')[0]
                maskIdTemp = segments[7]
                maskId = maskIdTemp.split('(')[1].split(')')[0]
                result = 'QCQueue', [self.wsName, logTime, gpuId, extract, occlusion, id, pose, attr, maskId]
        except:
            return result

        return result

    def processType1(self, infos):
        logTime = infos[2].split('.')[0]
        subType, results = self.parseType1(infos[-1], logTime)
        if results:
            if subType == 'frame':
                self.sdkFramePoolSizeOutput.append(results)
            elif subType == 'Queue':
                self.sdkQueueSizeOutput.append(results)
            elif subType == 'QCQueue':
                self.sdkQCQueueSizeOutput.append(results)

    def parseType2(self, message, logTime):
        segments = message.split(" ")
        try:
            # Type2: Callback process 0 ms. detect 0 ms, extract 0 ms, adv extract 0 ms.
            process = segments[2]
            detect = segments[5]
            extract = segments[8]
            advExtract = segments[12]
        except:
            return None

        return [self.wsName, logTime, process, detect, extract, advExtract]

    def processType2(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType2(infos[-1], logTime)
        if results:
            self.sdkEventTrackCallbackOutput.append(results)

    def parseType3(self, message, logTime):
        segments = message.split(" ")
        try:
            # Type3: Callback process 0 ms.
            process = segments[3]
        except:
            return None

        return [self.wsName, logTime, process]

    def processType3(self, infos):
        logTime = infos[2].split('.')[0]
        results = self.parseType3(infos[-1], logTime)
        if results:
            self.sdkTrackCallbackOutput.append(results)

    def getInfoType(self, infos):
        ## example
        # Type0: Enlarge frame pool size to 1557 success.
        # Type1: GPU 0 frame pool status available frame 14/420 for camera count 15
        #        GPU 0 Queue: Available:(27), Queued:(1), Processing:(100), Detect:(3), Extract:(97).
        #        GPU 0 Extract:(97), Occlusion(0), Id:(0), Pose:(0), Attr:(0), MaskId:(0).
        # Type2: Callback process 0 ms. detect 0 ms, extract 0 ms, adv extract 0 ms.
        # Type3: Callback process 0 ms.

        infoTypeDict = {
            "FaceMeSDK::FaceMeVideoFrameAllocator::AllocateFrame": '0',
            "FaceMeSDK::FaceMeCameraPersonReidRecognizer::ThreadProc": '1',
            "FaceMeSDK::FaceMeCameraPersonReidRecognizer::OnEventTrackFinish": '2',
            "FaceMeSDK::FaceMeCameraPersonReidRecognizer::OnTrackFinish": '3'
        }
        if infos[1] == "info" or infos[1] == "warning":
            return infoTypeDict[infos[5]] if infos[5] in infoTypeDict else None

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
        if len(infos) < 7:
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


def write_frame_buffer_size_file(filename, workstations, sdkFrameBufferSizeOutputList):
    wsMaxFrameBufferSizeDict = dict()
    for ws in workstations:
        wsMaxFrameBufferSizeDict[ws] = ['', '', '0']
    for ws, t, s in sdkFrameBufferSizeOutputList:
        if ws in workstations:
            if s > int(wsMaxFrameBufferSizeDict[ws][2]):
                wsMaxFrameBufferSizeDict[ws] = [ws, t, str(s)]
    header = ['time', 'maxFrameBufferSize']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(header)

        for ws, value in sorted(wsMaxFrameBufferSizeDict.items()):
            csv_writer.writerow(value)


def write_frame_pool_size_file(filename, workstations, sdkFramePoolSizeOutputList):
    wsMaxFramePoolSizeDict = dict()
    wsFramePoolSizeDict = dict()
    for ws in workstations:
        wsFramePoolSizeDict[ws] = dict()
        wsMaxFramePoolSizeDict[ws] = dict()
    for ws, t, gid, count, total, camCount in sdkFramePoolSizeOutputList:
        if ws in workstations:
            if gid not in wsMaxFramePoolSizeDict[ws].keys():
                wsFramePoolSizeDict[ws][gid] = [[ws, t, gid, count, total, camCount], ]
                wsMaxFramePoolSizeDict[ws][gid] = [ws, t, gid, count, camCount]
            else:
                wsFramePoolSizeDict[ws][gid].append([ws, t, gid, count, total, camCount])
                if int(count) > int(wsMaxFramePoolSizeDict[ws][gid][3]):
                    wsMaxFramePoolSizeDict[ws][gid] = [ws, t, gid, count, camCount]
    headerMax = ['WS', 'time', 'GPU ID', 'max frame pool size', 'camera count']
    header = ['WS', 'time', 'GPU ID', 'frame pool usage', 'frame pool size', 'camera count']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(headerMax)
        for ws, gid_dict in sorted(wsMaxFramePoolSizeDict.items()):
            for gid, value in sorted(gid_dict.items()):
                csv_writer.writerow(value)

        csv_writer.writerow(header)
        for ws, gid_dict in sorted(wsFramePoolSizeDict.items()):
            for gid, frame_list in sorted(gid_dict.items()):
                for value in frame_list:
                    csv_writer.writerow(value)


def write_queue_size_file(filename, sdkQueueSizeOutputList):
    # GPU 0 Queue: Available:(27), Queued:(1), Processing:(100), Detect:(3), Extract:(97).
    maxDict = dict()
    queueDict = dict()
    for ws, t, gid, ava, que, pro, det, ext in sdkQueueSizeOutputList:
        if gid not in maxDict.keys():
            maxDict[gid] = [ava, que, pro, det, ext]
            queueDict[gid] = [[ws, t, gid, ava, que, pro, det, ext]]
        else:
            queueDict[gid].append([ws, t, gid, ava, que, pro, det, ext])
            if int(ava) > int(maxDict[gid][0]):
                maxDict[gid][0] = ava
            if int(que) > int(maxDict[gid][1]):
                maxDict[gid][1] = que
            if int(pro) > int(maxDict[gid][2]):
                maxDict[gid][2] = pro
            if int(det) > int(maxDict[gid][3]):
                maxDict[gid][3] = det
            if int(ext) > int(maxDict[gid][4]):
                maxDict[gid][4] = ext

    headerMax = ['gpuId', 'available', 'queued', 'processing', 'detect', 'extract']
    header = ['WS', 'time', 'gpuId', 'available', 'queued', 'processing', 'detect', 'extract']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(headerMax)
        for gid, value in sorted(maxDict.items()):
            csv_writer.writerow([gid] + value)

        csv_writer.writerow([])

        csv_writer.writerow(header)
        for gid, values in sorted(queueDict.items()):
            for value in values:
                csv_writer.writerow(value)


def write_qc_queue_size_file(filename, sdkQCQueueSizeOutputList):
    # GPU 0 Extract:(97), Occlusion(0), Id:(0), Pose:(0), Attr:(0), MaskId:(0).
    maxDict = dict()
    queueDict = dict()
    for ws, t, gid, ext, occ, id, pose, attr, mid in sdkQCQueueSizeOutputList:
        if gid not in maxDict.keys():
            maxDict[gid] = [ext, occ, id, pose, attr, mid]
            queueDict[gid] = [[ws, t, gid, ext, occ, id, pose, attr, mid]]
        else:
            queueDict[gid].append([ws, t, gid, ext, occ, id, pose, attr, mid])
            if int(ext) > int(maxDict[gid][0]):
                maxDict[gid][0] = ext
            if int(occ) > int(maxDict[gid][1]):
                maxDict[gid][1] = occ
            if int(id) > int(maxDict[gid][2]):
                maxDict[gid][2] = id
            if int(pose) > int(maxDict[gid][3]):
                maxDict[gid][3] = pose
            if int(attr) > int(maxDict[gid][4]):
                maxDict[gid][4] = attr
            if int(mid) > int(maxDict[gid][5]):
                maxDict[gid][5] = mid

    headerMax = ['gpuId', 'extract', 'occlusion', 'id', 'pose', 'attr', 'maskId']
    header = ['WS', 'time', 'gpuId', 'extract', 'occlusion', 'id', 'pose', 'attr', 'maskId']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(headerMax)
        for gid, value in sorted(maxDict.items()):
            csv_writer.writerow([gid] + value)

        csv_writer.writerow([])

        csv_writer.writerow(header)
        for gid, values in sorted(queueDict.items()):
            for value in values:
                csv_writer.writerow(value)


def write_event_track_callback_file(filename, sdkEventTrackCallbackOutputList):
    # Type2: Callback process 0 ms. detect 0 ms, extract 0 ms, adv extract 0 ms.
    maxList = [0, 0, 0, 0]
    for ws, t, pro, det, ext, adv in sdkEventTrackCallbackOutputList:
        if int(pro) > int(maxList[0]):
            maxList[0] = pro
        if int(det) > int(maxList[1]):
            maxList[1] = det
        if int(ext) > int(maxList[2]):
            maxList[2] = ext
        if int(adv) > int(maxList[3]):
            maxList[3] = adv

    headerMax = ['process', 'detect', 'extract', 'adv extract']
    header = ['WS', 'time', 'process', 'detect', 'extract', 'adv extract']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(headerMax)
        csv_writer.writerow(maxList)

        csv_writer.writerow([])

        csv_writer.writerow(header)
        for value in sdkEventTrackCallbackOutputList:
            csv_writer.writerow(value)


def write_track_callback_file(filename, sdkTrackCallbackOutputList):
    # Type3: Callback process 0 ms.
    maxList = [0]
    for ws, t, pro in sdkTrackCallbackOutputList:
        if int(pro) > int(maxList[0]):
            maxList[0] = pro

    headerMax = ['process']
    header = ['WS', 'time', 'process']
    with open(filename, 'wb') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(headerMax)
        csv_writer.writerow(maxList)

        csv_writer.writerow([])

        csv_writer.writerow(header)
        for value in sdkTrackCallbackOutputList:
            csv_writer.writerow(value)


def find_log_files(folder_path):
    log_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".log") and "FaceMeSDK" in file:
                log_files.append(os.path.join(root, file))
    return log_files


def main():
    workstations = []
    for entry in os.listdir(folder_path):
        if os.path.isdir(os.path.join(folder_path, entry)):
            workstations.append(entry)
    print(workstations)

    sdkFrameBufferSizeOutputList = []
    sdkFramePoolSizeOutputList = []
    for workstation in workstations:
        log_files = find_log_files(os.path.join(folder_path, workstation))
        print(log_files)

        sdkQueueSizeOutputList = []
        sdkQCQueueSizeOutputList = []
        sdkEventTrackCallbackOutputList = []
        sdkTrackCallbackOutputList = []

        for log_file in log_files:
            print("processing file " + log_file)
            parser = parse_ws_log(workstation, LOG_START_TIME, LOG_END_TIME)
            parser.parseFile(log_file)

            sdkFrameBufferSizeOutputList += parser.sdkFrameBufferSizeOutput
            sdkFramePoolSizeOutputList += parser.sdkFramePoolSizeOutput
            sdkQueueSizeOutputList += parser.sdkQueueSizeOutput
            sdkQCQueueSizeOutputList += parser.sdkQCQueueSizeOutput
            sdkEventTrackCallbackOutputList += parser.sdkEventTrackCallbackOutput
            sdkTrackCallbackOutputList += parser.sdkTrackCallbackOutput

            # write csv for queue size
            outputFile3 = os.path.join(folder_path, workstation + "_queueSize.csv")
            if sdkQueueSizeOutputList:
                write_queue_size_file(outputFile3, sdkQueueSizeOutputList)

            # write csv for QC queue size
            outputFile4 = os.path.join(folder_path, workstation + "_qcQueueSize.csv")
            if sdkQCQueueSizeOutputList:
                write_qc_queue_size_file(outputFile4, sdkQCQueueSizeOutputList)

            # write csv for event track callback
            outputFile5 = os.path.join(folder_path + workstation + "_eventTrackCallback.csv")
            if sdkEventTrackCallbackOutputList:
                write_event_track_callback_file(outputFile5, sdkEventTrackCallbackOutputList)

            # write csv for track callback
            outputFile6 = os.path.join(folder_path, workstation + "_trackCallback.csv")
            if sdkTrackCallbackOutputList:
                write_track_callback_file(outputFile6, sdkTrackCallbackOutputList)

    # write csv for frame buffer size
    outputFile = os.path.join(folder_path, "frameBufferSize.csv")
    if sdkFrameBufferSizeOutputList:
        write_frame_buffer_size_file(outputFile, workstations, sdkFrameBufferSizeOutputList)

    # write csv for frame pool size
    outputFile2 = os.path.join(folder_path, "framePoolSize.csv")
    if sdkFramePoolSizeOutputList:
        write_frame_pool_size_file(outputFile2, workstations, sdkFramePoolSizeOutputList)


main()
