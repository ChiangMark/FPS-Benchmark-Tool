import configparser
import datetime
import json
import os
import psutil
import subprocess
import time
import logging
import threading
import pymssql
from selenium import webdriver
from FPS_Cal import FPS_cal
from QueryDB import querydb
from Create_DB import Auto_DB,Modify_DB
from sqlalchemy import create_engine,text
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from get_latest_log import get_all_log

with open('time.txt', 'w') as file:
    pass

class AutoFpsBenchmark():
    def __init__(self):
        self.sysconfig = r'C:\ProgramData\CyberLink\FaceMeSDK\FaceMeSecurityWorkstation\1000\Default\SystemConfig.json'
        self.fps_file_path = r'C:\ProgramData\CyberLink\FaceMeSDK\FaceMeSecurityWorkstation\1000\Default\CyberLink\FaceMeSDK\profile_report' 
        self.target_fps = 10
        self.dict_fps = {}
        self.run_time = 0
        self.device_name = subprocess.check_output(['hostname']).split()[-1].decode()
        # Log module
        self.log_module()
        # Delete gpuid file
        self.kill_workstation()
        print('Deleting gpuid_file...')
        self.delete_gpuid_file()
        #Wakeup workstation
        self.wakeup_workstation()
    def log_module(self):
        #Check log folder
        self.log_path = os.path.join(os.getcwd(),'log')
        if os.path.isdir(self.log_path):
            pass
        else:
            os.makedirs(self.log_path)
        self.formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] [%(name)s] [%(message)s]',datefmt='%Y-%m-%d %I:%M:%S')
        #root logger
        self.root_logger = logging.getLogger()
        self.root_logger.setLevel(logging.INFO)
        self.root_handler = logging.FileHandler(os.path.join(self.log_path,'auto_fps.log'),mode='a')
        self.root_handler.setFormatter(self.formatter)
        self.root_logger.addHandler(self.root_handler)
        #Selenium logger
        self.logger = logging.getLogger('Selenium')
        self.logger.propagate = False
        self.logger.setLevel(logging.ERROR)
        self.file_handler = logging.FileHandler(os.path.join(self.log_path,'Exception.log'),mode='a')
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
    def check_db(self):
        # Create DB if it doesn't exist
        db = Auto_DB()
        self.db_code = db.create_db() 
        if self.db_code:        
            self.root_logger.info('Create AutoFPSbenchmark DB success')   
            self.root_logger.info('Create FPS_result table success')          
        else:
            self.root_logger.info('AutoFPSbenchmark DB already exists')
    def check_profilereport(self):
        """read the config.ini"""
        #Check if profilereport=1 in the Sysconfig
        with open (self.sysconfig,'r') as f:
            config_file = json.load(f)
            for key,value in config_file.items():
                ws_key = key
                ws_name = value
            if 'ProfileReport' not in config_file.keys():
                f.close()
                with open (self.sysconfig,'w') as w:
                    data = {ws_key : ws_name,"ProfileReport" : 1}
                    json.dump(data,w,indent='\t')
            else:
                pass
        self.root_logger.info('Check profile report: OK!')
    def check_hw_usage(self):
        #Check NV GPu driver
        try:
            subprocess.run('nvidia-smi',creationflags=subprocess.CREATE_NO_WINDOW)
            self.nv_gpu = 1
        except FileNotFoundError:
            self.nv_gpu = 0
        #CPU info
        self.cpu_info = ''
        cpu_info_list = [cpu_name.decode('utf-8') for cpu_name in subprocess.check_output(['wmic','cpu','get','NAME']).split()[1:]]
        for cpu_info in cpu_info_list:
            self.cpu_info = self.cpu_info + cpu_info + ' '
        if self.nv_gpu:
            #GPU info
            self.gpu_name = ''
            gpu_name_list = [gpu_name.decode('utf-8') for gpu_name in subprocess.check_output(['nvidia-smi','--query-gpu=name','--format=csv']).split()[1:]]
            for gpu_name in gpu_name_list:
                self.gpu_name = self.gpu_name + gpu_name + ' '
            self.gpu_memroy = subprocess.check_output(['nvidia-smi','--query-gpu=memory.total','--format=csv']).split()[-2].decode('utf-8')
            self.gpu_memroy = int(int(self.gpu_memroy) / 1024)
            self.gpu = 0
            self.ws_gpu_usage = 0
        self.cpu = 0     
        count = 0
        for proc in psutil.process_iter():
            if proc.name() == 'FaceMeSecurityWorkstation.exe':
                self.ws_pid = proc.pid
        for i in range(int(self.waiting_time/10)):    
            #CPU usage
            self.cpu_usage = psutil.cpu_percent(interval=10)
            self.cpu += self.cpu_usage
            if self.nv_gpu:
                #Dedicated GPU usage
                self.gpu_usage = round(float(subprocess.check_output(['nvidia-smi','--query-gpu=memory.used','--format=csv']).split()[-2].decode('utf-8'))/1024,2)
                self.gpu += self.gpu_usage
                #Workstation dedicated GPU usage
                command = '''Get-Counter "\GPU Process Memory(pid_{}*)\Dedicated Usage"'''.format(self.ws_pid)
                self.ws_gpu_shell = subprocess.run(['powershell','-command',command],capture_output=True).stdout.decode('utf-8').strip('\r\n')
                self.ws_gpu_usage_temp = self.ws_gpu_shell.split('\r\n')
                for info in reversed(self.ws_gpu_usage_temp):
                    if info.strip() != '':
                        self.ws_gpu_usage += float(info.strip())
                        break
            count += 1
        #Output to console    
        self.cpu_avg = round(self.cpu / count,2)   
        print('CPU Info'.center(80, '=')+'\n'+'CPU Device: {}'.format(self.cpu_info)+'\n'+'CPU usage: {}%'.format(self.cpu_avg))
        if self.nv_gpu:
            self.ws_gpu_usage = round(self.ws_gpu_usage/pow(1024,3),2)
            self.gpu_avg = round(self.gpu / count,2)
            self.ws_gpu_avg = round(self.ws_gpu_usage / count,2)
            print('GPU info'.center(
                80, '=')+'\n'+'GPU Device: {}, Memory: {} GB'.format(self.gpu_name,self.gpu_memroy)+'\n'+'GPU uasge: {} GB'.format(self.gpu_avg)+'\n'+'Workstation dedicated GPU usage: {} GB'.format(self.ws_gpu_avg))       
    def get_config_info(self):
        self.check_profilereport()
        self.check_db()
        #Create config object
        self.config = configparser.ConfigParser()
        self.config.read('config.ini',encoding='utf-8')
        #Get settings info
        #Central
        self.ct_ip = self.config['Basic_info']['Central_ip']
        self.ct_acct = self.config['Basic_info']['Central_account']
        self.ct_pw = self.config['Basic_info']['Central_password']
        #DB
        self.db_ip = self.config['DBinfo']['db_ip']
        self.db_acct = self.config['DBinfo']['db_acct']
        self.db_pw = self.config['DBinfo']['db_pw']
        #fps benchmark settings
        self.fps_boundary = self.config['FPSbenchmark'].getint('fps_boundary')
        self.waiting_time = self.config['FPSbenchmark'].getfloat('wait_min') * 60
        #Add camera info
        self.rtsp = self.config['Add_cams']['rtsp_url']
        self.rtsp = self.rtsp.split(',')
        self.open_cam = self.config['Add_cams'].getint('num_of_start_cams')
        self.total_cam = self.config['Add_cams'].getint('num_of_cams')
        self.cam_prefix =  self.config['Add_cams']['prefix_of_name']
        #Media Sever Info
        self.media = self.config['Media_Server']['Media_Server_folder_path']
        #Previous IP cam number
        self.pre_cam = self.config['Previous_results'].getint('cam_num')
        #Workstation relay
        self.ws_relay = self.config['Workstation_relay'].getint('mode')
        if self.ws_relay:
            self.ws_name = self.config['Workstation_relay']['ws_name']
        else:
            self.ws_name = self.device_name
        #Opening media_server
        self.media_proc = subprocess.Popen('start.bat',cwd=self.media,creationflags=subprocess.CREATE_NO_WINDOW,shell=True)
        print('Opening media server successful')
        self.root_logger.info('Opening media server')
    def kill_workstation(self):
        #Kill Workstation Agent
        self.kill_process = ['FaceMeSecurityWorkstationAgent.exe','FaceMeSecurityWorkstation.exe']
        for program in psutil.process_iter():
            if program.name() in self.kill_process:
                program.kill()
            else:
                pass
        time.sleep(3)
        self.root_logger.info('Kill Workstation agent')
    def wakeup_workstation(self):
        #Check WorkStationAgent is running
        if [process for process in psutil.process_iter() if process.name() == 'FaceMeSecurityWorkstationAgent.exe']:
            pass
        else:
            psutil.Popen(r'C:\Program Files\CyberLink\FaceMeSecurityWorkstation\FaceMeSecurityWorkstationAgent.exe',shell=True)
            print('Opening FaceMeSecurityWorkstationAgent... Please wait for a while.')
            time.sleep(5)
        self.root_logger.info('Waking up Workstation')
    def Add_cams(self):
        #Connect to Database
        self.conn_engine = create_engine('mssql+pymssql://{}:{}@{}/faceme_security'.format(self.db_acct,self.db_pw,self.device_name),connect_args={'autocommit':True})
        self.conn = self.conn_engine.connect()
        #Delete all cams first for initialize
        self.conn.execute(text('DELETE [faceme_security].[dbo].[camera_info]'))
        self.root_logger.info('Delete all cams successfully')
        #Chrome driver setting
        chrome_option = webdriver.ChromeOptions()
        chrome_option.add_argument('--headless')
        chrome_option.add_experimental_option('detach', True)
        chrome_option.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.driver = webdriver.Chrome(options=chrome_option)
        try:
            #Get to Central page
            self.driver.get('http://%s:8080/workstations'%self.ct_ip)
            self.driver.maximize_window()
            self.root_logger.info('Go to Central page')
            #Log in
            WebDriverWait(self.driver,5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="root"]/div/div/div/div/div/div/form/div[1]/div/input'))).send_keys(self.ct_acct)
            WebDriverWait(self.driver,5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="root"]/div/div/div/div/div/div/form/div[2]/div/input'))).send_keys(self.ct_pw)
            WebDriverWait(self.driver,5).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'span.MuiButton-label'))).click()
            self.root_logger.info('Log in successfully')
            #Click Workstation
            WebDriverWait(self.driver,5).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'''button[tabindex="0"]'''))).click()
            #Click workstation to add ip cam
            self.ws_page = WebDriverWait(self.driver,5).until(EC.element_to_be_clickable((By.LINK_TEXT,'%s'%self.ws_name)))
            self.driver.execute_script('arguments[0].click();',self.ws_page)
            self.root_logger.info('Ready to add %s cams'%self.open_cam)
            #Check multiple rtsp
            self.init_num = 1
            self.end_num = 21
            if self.total_cam % 20 == 0:
                self.loop = int(self.total_cam/20)
            else:
                if int(self.total_cam) < 20:
                    self.loop = 1
                    self.end_num = self.total_cam + 1
                else:
                    self.loop = int(self.total_cam/20) + 1
            for loop_num in range(self.loop):
                for number in range(self.init_num,self.end_num):
                    # Click [Add Camera]
                    self.add_cam_page = WebDriverWait(self.driver,5).until(EC.element_to_be_clickable((By.CSS_SELECTOR,'span.MuiButton-label')))
                    self.driver.execute_script('arguments[0].click();',self.add_cam_page)
                    # Input IPcam name
                    self.driver.find_elements(By.CSS_SELECTOR,'input.MuiInputBase-input.MuiOutlinedInput-input')[0].send_keys(self.cam_prefix + str(number))
                    # Input IPcam location
                    self.driver.find_elements(By.CSS_SELECTOR,'input.MuiInputBase-input.MuiOutlinedInput-input')[1].send_keys("q")
                    # Input rtsp
                    self.driver.find_elements(By.CSS_SELECTOR,'input.MuiInputBase-input.MuiOutlinedInput-input')[2].send_keys('%s?%s'%(self.rtsp[loop_num],number))
                    # Click [Add]
                    self.driver.find_element(By.XPATH,'/html/body/div/div[3]/div/form/div[2]/div/div[2]/button').click()
                    time.sleep(1)
                if self.loop == 1:
                    break
                else:
                    self.init_num += 20
                    self.end_num += 20
                    if self.end_num >= self.total_cam:
                        self.end_num = self.total_cam +1
                    else:
                        pass
            self.root_logger.info('Adding %s IP cams completely'%self.total_cam)
        except Exception:
            self.logger.exception('Message')
            print('Some errors occur. Application will be closed.')
            AutoFpsBenchmark.closing_app(self,1)
    
    def open_cams(self):
        if self.ws_relay == 1:
            self.ws_id = self.conn.execute(text('''SELECT [workstation_id] FROM [faceme_security].[dbo].[workstation_info] WHERE [name]= '{}' '''.format(self.ws_name))).fetchone()[0]
            self.conn.execute(text('''UPDATE [faceme_security].[dbo].[camera_info] SET central_id=Null, relay_on_workstation_id={}'''.format(self.ws_id)))
            self.root_logger.info('Workstation_name:{}, Workstation ID: {}'.format(self.ws_name,self.ws_id))
            self.root_logger.info('Change to Workstation relay')
        print('Opening %s IP cams...'%self.open_cam)
        for cam in range(1,self.open_cam+1):
            self.conn.execute(text('''UPDATE [faceme_security].[dbo].[camera_info] SET [is_active]=1 WHERE [name]='%s' '''%(self.cam_prefix+str(cam))))
        self.root_logger.info('Opening %s IP-cams'%self.open_cam)
    def wait_for_run(self):
        if not self.run_time:
            self.Add_cams()
            self.open_cams()
        elif self.run_time:
            self.open_cams()
        self.cam_code = 1
        while self.cam_code:
            #Check for connecting successfully
            self.cam_status = self.conn.execute(text('''SELECT [name] FROM [faceme_security].[dbo].[camera_info] WHERE [status] = 'MONITORING_ON' '''))
            self.cam_on = self.cam_status.fetchall()
            if len(self.cam_on) == self.open_cam:
                print('''All of the IP cams connect successfully\nPlease wait %s minute(s) for running...'''%(int(self.waiting_time/60)))
                self.cam_code = 0
            else:
                if (self.open_cam - len(self.cam_on)) == 1:
                    print('''%s IP cam isn't connected'''%(self.open_cam - len(self.cam_on)))
                else:
                    print('''%s IP cams aren't connected'''%(self.open_cam - len(self.cam_on)))
                time.sleep(15)    
        self.root_logger.info('All cameras connected')
        self.root_logger.info('Waiting for %s minutes running'%self.config['FPSbenchmark']['wait_min'])
        #Start running&output HW usage
        self.hw_thread = threading.Thread(target=self.check_hw_usage)   
        self.hw_thread.run()
        self.run_time += 1
        #Close all cameras > Stop running
        self.conn.execute(text('UPDATE [faceme_security].[dbo].[camera_info] SET [is_active]=0'))
        self.root_logger.info('Close all cams')
        self.kill_workstation()
    def output_fps(self):
        #Depand on the demand to calculate FPS
        calculate = FPS_cal()
        self.fps = calculate.cal()
        print('FPS results'.center(80,'=')+'\n'+'%s cams average fps: %s'%(self.open_cam,self.fps))   
        self.root_logger.info('Calculating average FPS complete')   
    def delete_gpuid_file(self):
        if os.path.isdir(self.fps_file_path):
            self.gpu_file = os.listdir(self.fps_file_path)
            for rm_file in self.gpu_file:
                os.remove(os.path.join(self.fps_file_path,rm_file))
        else:
            os.makedirs(self.fps_file_path)
    def add_to_db(self):
        #Export to txt
        with open('Results.txt','a') as f:
            f.write('FPS_Results'.center(80,'=') + '\n' + '%s, %s IP Cams: %s fps'%(self.time_now.strftime('%Y/%m/%d %H:%M:%S'),self.open_cam,self.fps) + '\n')
            f.write('CPU info'.center(80, '=')+'\n'+'CPU Device: {}'.format(self.cpu_info)+'\n'+'CPU usage: {}%'.format(self.cpu_avg)+'\n')
            if self.nv_gpu: 
                f.write('GPU info'.center(80, '=')+'\n'+'GPU Device: {}, Memory: {} GB'.format(self.gpu_name,self.gpu_memroy)+'\n'+'GPU uasge: {} GB'.format(self.gpu_avg)+'\n'+'Workstation dedicated GPU usage: {} GB'.format(self.ws_gpu_avg)+'\n')
        self.root_logger.info('Export FPS data and Hardware usage')
        #Export to DB
        self.data = Auto_DB(cam_number=self.open_cam,FPS_result=self.fps,Time=self.time_now.strftime('%Y/%m/%d %H:%M:%S'))
        modify_obj = Modify_DB()
        modify_obj.insert_data(self.data)
        self.root_logger.info('Export data to Auto_DB')
    def logic(self):
        self.output_fps_thread = threading.Thread(target=self.output_fps)
        self.output_fps_thread.run()
        #Check to exit or not
        if self.open_cam not in self.dict_fps.keys():
            self.dict_fps[self.open_cam] = self.fps
            self.time_now = datetime.datetime.now()    
            self.insert_to_DB_thread = threading.Thread(target=self.add_to_db)
            self.insert_to_DB_thread.run()
        else:
            if self.dict_fps[self.open_cam] < self.fps:
                self.dict_fps[self.open_cam] = self.fps                 
            else:
                self.export_fps_thread = threading.Thread(target=self.fps_output,args=self.dict_fps)
                self.export_fps_thread.run()
        #Next open cams number
        if 3 < self.fps - self.target_fps < 5:
            self.open_cam += 3
        elif 2 < self.fps - self.target_fps < 3:
            self.open_cam += 2
        elif 1 < self.fps - self.target_fps < 2:
            self.open_cam +=1
        elif 0 < self.fps - self.target_fps < 1:
            self.open_cam +=1
        elif -5 < self.fps - self.target_fps < -3:
            self.open_cam -= 3
        elif -3 < self.fps - self.target_fps < -2:
            self.open_cam -= 2
        elif -2 < self.fps - self.target_fps < -1:
            self.open_cam -= 1
        elif -1 < self.fps - self.target_fps < -0:
            self.open_cam -= 1
        else:
            print('Info'.center(80,'=')+'\n'+'The hardware is not powerful. Please change it and restart it again.'+'\n'+''.center(80,'='))
            self.closing_app()
        if self.open_cam >= self.total_cam:
            print('Info'.center(80,'=')+'\n'+'Please reset the total cams. The totoal cams are not enough.'+'\n'+''.center(80,'='))
            self.closing_app()
        elif self.open_cam <= 0:
            print('Info'.center(80,'=')+'\n'+'''The hardware efficiency isn't enough. Please change it and restart it again.'''+'\n'+''.center(80,'='))
            self.closing_app()
        else:
            pass
        return self.delete_gpuid_file(),self.wakeup_workstation(),self.wait_for_run(),self.logic()
    def fps_output(self,fps_dict):
        temp = {}
        for key,value in fps_dict.items():
            temp[key] = value - self.fps_boundary
        self.fps_out_key = min(temp,key=lambda x: temp[x] if temp[x] > 0 else 10)
        if self.fps_out_key < self.pre_cam:
            print('''Not same with previous IP cam's number\nWill do %s IP cams again for further check'''%(self.pre_cam))
            self.root_logger.info('''The results are %s IP cams which isn't same with previous one. Do %s IP cams again for further check.'''%(self.fps_out_key,self.pre_cam))
            self.open_cam = self.pre_cam
            self.delete_gpuid_file()
            self.open_cams()
            self.wait_for_run()
            self.output_fps()        
            if self.fps > 10:
                self.fps_out_key = self.open_cam
                fps_dict[self.fps_out_key] = self.fps   
                self.insert_to_DB_thread = threading.Thread(target=self.add_to_db)             
                self.insert_to_DB_thread.run()
        print('Results'.center(80,'=')+'\n'+'The result fps is %s of %s IP Cams.'%(fps_dict[self.fps_out_key],self.fps_out_key))
        self.root_logger.info('FPS benchmark process done')
        self.closing_app()
    def closing_app(self,code=0):
        os._exit(code)

if __name__ == '__main__':
    # while True:
    #     print(psutil.cpu_percent(interval=2,percpu=True))
    #     print(psutil.cpu_percent())
    auto = AutoFpsBenchmark()
    auto.get_config_info()
    auto.wait_for_run() 
    DB = querydb()
    DB.get_camID()
    DB.get_time()
    DB.queryPersonIdCount()
    getinfo = get_all_log()
    getinfo.copy_latest_ct_log()
    os.system('py -2 parse_ct_log.py')
    # print("Wait 60s for merging...")
    # time.sleep(60)
    auto.wakeup_workstation()
    # auto = AutoFpsBenchmark()
    # auto.get_config_info()
    # auto.wait_for_run()
    # if not auto.ws_relay:
    #    auto.logic()