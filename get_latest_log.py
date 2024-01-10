import os
import shutil
import fnmatch
import configparser

config = configparser.RawConfigParser() 
config.read("config.ini")

class get_all_log:
    def __init__(self):
        self.destination_path = config["Path"]["target"]
        self.ct_new_log_path = config["Path"]["ct_faceme_new_log"]
        self.ct_old_log_path = config["Path"]["ct_faceme_old_log"]
        self.ws_log_path = config["Path"]["ws_log"]
        self.sdk_log_path =config["Path"]["sdk_log"]
        self.ws_target_pattern = r"*Workstation*.*"
        self.sdk_target_pattern = r"*FaceMeSDK*.*"
        self.ct_target_pattern = r"*faceme*.*"

    def copy_latest_ct_log(self):
         # 获取源文件夹中的所有文件
        all_files = os.listdir(self.ct_new_log_path)

        # 过滤出文件，排除文件夹
        files = [f for f in all_files if os.path.isfile(os.path.join(self.ct_new_log_path, f))]

        if not files:
            print("No files found in the source folder.")
            return

        # 在文件列表中进行模糊匹配
        matching_files = fnmatch.filter(files, self.ct_target_pattern)

        if not matching_files:
            print(f"No files matching the pattern '{self.ct_target_pattern}' found in the source folder.")
            return

        # 获取相似文件中最新修改时间的文件
        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(self.ct_new_log_path, f)))

        # 构建源文件和目标文件的路径
        source_path = os.path.join(self.ct_new_log_path, latest_file)
        destination_path = os.path.join(self.destination_path, latest_file)

        # 复制文件
        shutil.copy2(source_path, destination_path)
        print(f"Latest file '{latest_file}' matching the pattern '{self.ws_target_pattern}' copied from {self.ct_new_log_path} to {self.destination_path}.")
        
    def copy_ct_old_log(self):
         # 获取源文件夹中的所有文件
        all_files = os.listdir(self.ct_old_log_path)

        # 过滤出文件，排除文件夹
        files = [f for f in all_files if os.path.isfile(os.path.join(self.ct_old_log_path, f))]

        if not files:
            print("No files found in the source folder.")
            return

        # 在文件列表中进行模糊匹配
        matching_files = fnmatch.filter(files, self.ct_target_pattern)

        if not matching_files:
            print(f"No files matching the pattern '{self.ct_target_pattern}' found in the source folder.")
            return

        # 获取相似文件中最新修改时间的文件
        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(self.ct_old_log_path, f)))

        # 构建源文件和目标文件的路径
        source_path = os.path.join(self.ct_old_log_path, latest_file)
        destination_path = os.path.join(self.destination_path, latest_file)

        # 复制文件
        shutil.copy2(source_path, destination_path)
        print(f"Latest file '{latest_file}' matching the pattern '{self.ws_target_pattern}' copied from {self.ct_old_log_path} to {self.destination_path}.")
        
    def copy_ws_log(self):
         # 获取源文件夹中的所有文件
        all_files = os.listdir(self.ws_log_path)

        # 过滤出文件，排除文件夹
        files = [f for f in all_files if os.path.isfile(os.path.join(self.ws_log_path, f))]

        if not files:
            print("No files found in the source folder.")
            return

        # 在文件列表中进行模糊匹配
        matching_files = fnmatch.filter(files, self.ws_target_pattern)

        if not matching_files:
            print(f"No files matching the pattern '{self.ws_target_pattern}' found in the source folder.")
            return

        # 获取相似文件中最新修改时间的文件
        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(self.ws_log_path, f)))

        # 构建源文件和目标文件的路径
        source_path = os.path.join(self.ws_log_path, latest_file)
        destination_path = os.path.join(self.destination_path, latest_file)

        # 复制文件
        shutil.copy2(source_path, destination_path)
        print(f"Latest file '{latest_file}' matching the pattern '{self.ws_target_pattern}' copied from {self.ws_log_path} to {self.destination_path}.")
        
    def copy_sdk_log(self):
         # 获取源文件夹中的所有文件
        all_files = os.listdir(self.sdk_log_path)

        # 过滤出文件，排除文件夹
        files = [f for f in all_files if os.path.isfile(os.path.join(self.sdk_log_path, f))]

        if not files:
            print("No files found in the source folder.")
            return

        # 在文件列表中进行模糊匹配
        matching_files = fnmatch.filter(files, self.sdk_target_pattern)

        if not matching_files:
            print(f"No files matching the pattern '{self.sdk_target_pattern}' found in the source folder.")
            return

        # 获取相似文件中最新修改时间的文件
        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(self.sdk_log_path, f)))

        # 构建源文件和目标文件的路径
        source_path = os.path.join(self.sdk_log_path, latest_file)
        destination_path = os.path.join(self.destination_path, latest_file)

        # 复制文件
        shutil.copy2(source_path, destination_path)
        print(f"Latest file '{latest_file}' matching the pattern '{self.sdk_target_pattern}' copied from {self.sdk_log_path} to {self.destination_path}.")   
    
if __name__ == '__main__':
    getinfo = get_all_log()
    getinfo.copy_latest_ct_log()
    # getinfo.copy_ct_old_log()
    # getinfo.copy_ws_log()
    # getinfo.copy_sdk_log()



