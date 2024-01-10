class Update:
    def __init__(self):
        self.personId = None
        self.logDate = None
        self.faceId = None
        self.score = None
        self.boundingBox = None
        self.width = None
        self.occlusion = None

class Record:
    def __init__(self):
        self.logDate = None
        self.ticket = None
        self.recordId = None
        self.cameraId = None
        self.personId = None
        self.faceId = None
        self.score = None
        self.boundingBox = None
        self.width = None
        self.occlusion = None
        self.t01 = None
        self.t1 = None
        self.t2 = None
        self.t2r = None
        self.status = None
        self.updatedRecord = []

class RecordUpdate:
    def __init__(self):
        self.record = None
        self.updateCount = None
        self.duration = None
        self.outlierCount = None
        self.faceWidth = None
        self.score = None

class RecognizeTime:
    def __init__(self):
        self.amount = None
        self.summation = None
        self.maximum = None
        self.t0_2 = None
        self.t2_4 = None
        self.t4_10 = None
        self.t10_ = None

