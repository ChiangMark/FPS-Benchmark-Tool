import os

class FPS_cal(object):
    def __init__(self,path = r'C:\ProgramData\CyberLink\FaceMeSDK\FaceMeSecurityWorkstation\1000\Default\CyberLink\FaceMeSDK\profile_report'):
        self.path = path
    def cal(self):
        fps = 0
        count = 0
        for file in os.listdir(self.path):
            init = 1
            while (init < 10):
                with open(os.path.join(self.path,file),'rt') as r:       
                    text = r.readlines()[-init].strip().split(',')
                    if text.__len__() < 17 or text[-1].__len__() < 3:
                        init += 1
                        r.close()
                        continue
                    else:
                        fps += float(text[-1])
                        count += 1
                        break
        return round(fps/count,2)

if __name__ == '__main__':
    t = FPS_cal()
    print('Average FPS: {}'.format(t.cal()))