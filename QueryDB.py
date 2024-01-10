from datetime import datetime, timedelta
import pypyodbc as pyodbc
import datetime
import time
import configparser

config = configparser.RawConfigParser() #fix that url include %
config.read("config.ini")

class querydb():
    def __init__(self):
        self.UID = config["DBinfo"]["db_acct"]
        self.PWD = config["DBinfo"]["db_pw"]
        self.DATABASE = config["DBinfo"]["db_faceme"]
        self.SERVER = config["DBinfo"]["db_ip"]
        
        self.mydb = pyodbc.connect("DRIVER={SQL Server};"
                    f'SERVER={self.SERVER};'
                    f'UID={self.DATABASE};'
                    f'UID={self.UID};'
                    f'PWD={self.PWD};'
                    "TrustServerCertificate=yes;"
                    "Connection Timeout=60")
        self.mycursor = self.mydb.cursor()
        self.during_time = config["Media_Server"]["During_Time"]
        
    def get_camID(self):
        self.command = str("SELECT [camera_id] FROM [faceme_security].[dbo].[camera_info]")
        self.mycursor.execute(self.command)
        self.camID = (self.mycursor.fetchall())
        
    def get_time(self):
        self.camID1 = str(self.camID[-1][0])
        self.command = f"SELECT [log_time], DATEADD(second, {self.during_time}, log_time) AS log_time2 FROM [faceme_security].[dbo].[record] WHERE [camera_id]={self.camID1}"
        self.mycursor.execute(self.command)
        self.ID1time = (self.mycursor.fetchall())
        
        start_time = str(self.ID1time[0][0].strftime('%Y/%m/%d %H:%M:%S.%f')[:-3])
        end_time = str(self.ID1time[0][1].strftime('%Y/%m/%d %H:%M:%S.%f')[:-3])
        with open('time.txt', 'a') as file:
            file.write(start_time)
            file.write("\n")
            file.write(end_time)
            
    def get_camera_number(self):
        self.command = str("SELECT COUNT (camera_id) FROM [faceme_security].[dbo].[camera_info]")
        self.mycursor.execute(self.command)
        self.camera_number = (self.mycursor.fetchall())
            
    def get_face_count(self):
        self.camID2 =  str(self.camID[0][0])
        self.command = f"SELECT COUNT(*) FROM [faceme_security].[dbo].[record] WHERE [camera_id]={self.camID2} AND [log_time] BETWEEN '{self.ID1time[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' AND '{self.ID1time[0][1].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        self.mycursor.execute(self.command)
        self.finalfacerecord = self.mycursor.fetchall()
        print("face records: ", self.finalfacerecord[0][0])
        
    def get_body_count(self):
        self.camID2 =  str(self.camID[0][0])
        self.command = f"SELECT COUNT(*) FROM [faceme_security].[dbo].[body_record] WHERE [camera_id]={self.camID2} AND [log_time] BETWEEN '{self.ID1time[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}' AND '{self.ID1time[0][1].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        self.mycursor.execute(self.command)
        self.finalbodyrecord = self.mycursor.fetchall()
        print("body records: ", self.finalbodyrecord[0][0])

if __name__ == '__main__':
    DB = querydb()
    DB.get_camID()
    DB.get_time()
    DB.get_camera_number()
    DB.get_face_count()
    DB.get_body_count()
