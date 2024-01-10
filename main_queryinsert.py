import os
import gzip
import shutil

# Import the parseT2
import parseT2

LOG_START_TIME = 0
LOG_END_TIME = 24

def checkTime(logTime):
    hour = int(logTime.split(':')[0])
    return True if (hour>=LOG_START_TIME and hour<LOG_END_TIME) else False

def proc(central_log_path_list):
    record_info = dict()
    sum_all_elapsed_time_queryorinsert = 0
    count_all_queryorinsert = 0
    max_duration = 0
    stat_csv_order = [0, 300, 500, 2000, 5000, 10000]
    stat_dict = {0: 0, 300: 0, 500: 0, 2000: 0, 5000: 0, 10000: 0}

    parse_folder = central_log_path_list[0].split('.')[0] + "_queryinsert"
    try:
        # Use shutil.rmtree to remove the folder and its contents
        shutil.rmtree(parse_folder)
        print("Folder '{}' and its contents have been removed.".format(parse_folder))
    except Exception as e:
        pass
    if not os.path.exists(parse_folder):
        os.makedirs(parse_folder)
    result_file = os.path.join(parse_folder, 'results.txt')

    for central_log_path in central_log_path_list:
        if not os.path.isfile(central_log_path):
            return False, 0, None

        ticketMap = dict()

        file_reader = open(central_log_path, 'r')
        pattern = "URI:/api/workstation/face.queryorinsert"
        count = 0
        count_create = 0

        _start_time = None
        _last_time = None
        workstation_list = {}

        while True:
            count += 1
            line_string = file_reader.readline()
            if not line_string:
                break

            if line_string.find(pattern) < 0:
                #
                ret, ticketId, elapsed_time = parseT2.parse_queryorinsert_resp(line_string, ticketMap, record_info)
                if ticketId != None:
                    sum_all_elapsed_time_queryorinsert += elapsed_time
                    count_all_queryorinsert += 1
                    if elapsed_time > max_duration:
                        max_duration = elapsed_time
                    if elapsed_time < 300:
                        stat_dict[0] += 1
                    elif elapsed_time < 500:
                        stat_dict[300] += 1
                    elif elapsed_time < 2000:
                        stat_dict[500] += 1
                    elif elapsed_time < 5000:
                        stat_dict[2000] += 1
                    elif elapsed_time < 10000:
                        stat_dict[5000] += 1
                    else:
                        stat_dict[10000] += 1
                continue

            # parse timecode
            split_str = line_string.split(" [qtp")
            logtime = split_str[0]
            line_string = split_str[1]
            if not checkTime(logtime.split()[1]):
                continue

            # parse log_id
            pos = line_string.find("]")
            log_id = "qtp" + line_string[:pos]
            line_string = line_string[pos+1:]

            # parse ticket
            prefix_ticket = "RequestLogFilter"
            postfix_ticket = "]"
            pos = line_string.find(prefix_ticket)
            split_ticket_str = line_string[pos:].split("[")
            ticketMap[log_id] = ""
            ticketId = ""
            if len(split_ticket_str) > 1:
                split_ticket_str = split_ticket_str[1].split("]")
                ticketMap[log_id] = split_ticket_str[0]
                ticketId = split_ticket_str[0]

            # parse record.create  (T1-T1R = elapsed_time)
            # Log1 - folder "classification_alllogs" (classify record.create by classification_alllogs/{workstationid}/{camId}_log.csv)
            # Log2 - folder "folder_details_logs_delayTime" (classify T1-T1R by folder_details_logs_delayTime/WS{workstationid}/{delaytime}s.csv)
            list_find_api = line_string.split("face.queryorinsert PostParams:")
            if len(list_find_api) <= 1:
                continue

            # parse IP
            IP = "*.*.*.*"
            ipStr = list_find_api[0].split("IP:")
            if len(ipStr) > 1:
                IP = ipStr[1].split()[0]

            record_info[ticketId] = ticketId


    # Generate Summary files
    ave_result = "%.2f" % (sum_all_elapsed_time_queryorinsert * 1.0 / count_all_queryorinsert)
    moreInfo = "Result => \nsum: {}ms, count: {}".format(sum_all_elapsed_time_queryorinsert, count_all_queryorinsert)
    whole_context = ''
    whole_context += "sum: {} \n".format(sum_all_elapsed_time_queryorinsert)
    whole_context += "count: {} \n".format(count_all_queryorinsert)
    whole_context += "avg.: {}\n".format(ave_result)
    whole_context += "max.: {}\n".format(max_duration)
    whole_context += "stat.:\n"
    for duration in stat_csv_order:
        _period_data = stat_dict[duration]
        whole_context += "{}, {}, {}\n".format(duration, _period_data, "%.2f" % (_period_data * 1.0 / count_all_queryorinsert))
    with open(result_file, 'w') as logfile_s_writter:
        logfile_s_writter.write(whole_context)


    return True, ave_result, moreInfo, sum_all_elapsed_time_queryorinsert, count_all_queryorinsert, max_duration, stat_dict, whole_context


def unzipfile(gz_file_path):

    print("unzip file {} ....".format(gz_file_path))
    # Specify the path for the extracted file
    extracted_file_path = gz_file_path.split('.gz')[0]

    # Open the GZ file and extract its contents
    with gzip.open(gz_file_path, 'rb') as gz_file:
        with open(extracted_file_path, 'wb') as extracted_file:
            shutil.copyfileobj(gz_file, extracted_file)


def run_instance(log_file_list):
    print("parsing {} (queryorinsert ave. elapsed time)....".format(str(log_file_list)))
    ret, elapsed_time, moreInfo, sum_duration, count_duration, max_duration, stat_dict, whole_context = proc(log_file_list)
    print(moreInfo)
    print("elapsed_time:" + elapsed_time + "ms")
    print("max_duration:" + str(max_duration) + "ms")
    print("sorted_list:")
    print([(duration, stat_dict[duration]) for duration in sorted(stat_dict)])
    return sum_duration, count_duration, max_duration, stat_dict, whole_context

def main_queryinsert():
    directory_path = "logs"
    log_file_lists = []

    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            if filename.endswith(".gz"):
                if filename[:filename.find(".gz")] not in files:
                    unzipfile(os.path.join(root, filename))

    for root, dirs, files in os.walk(directory_path):
        file_list = []
        for filename in files:
            if filename.endswith(".log") and filename.startswith("faceme"):
                file_list.append(os.path.join(root, filename))
        if file_list:
            log_file_lists.append(file_list)

    total_whole_context = ''
    total_sum_duration = 0
    total_count_duration = 0
    total_max_duration = 0
    total_stat_csv_order = [0, 300, 500, 2000, 5000, 10000]
    total_stat_dict = {0: 0, 300: 0, 500: 0, 2000: 0, 5000: 0, 10000: 0}
    for log_file_list in log_file_lists:
        sum_duration, count_duration, max_duration, stat_dict, whole_context = run_instance(log_file_list)
        total_sum_duration += sum_duration
        total_count_duration += count_duration
        if max_duration > total_max_duration:
            total_max_duration = max_duration
        for key in stat_dict.keys():
            total_stat_dict[key] += stat_dict[key]
        total_whole_context += 'log file list: ' + str(log_file_list) + '\n'
        total_whole_context += whole_context
        total_whole_context += '\n\n'

    total_context = ''
    if total_count_duration > 0:
        total_context += "sum, {} \n".format(total_sum_duration)
        total_context += "count, {} \n".format(total_count_duration)
        ave_result = "%.2f" % (total_sum_duration * 1.0 / total_count_duration)
        total_context += "avg., {}\n".format(ave_result)
        total_context += "max., {}\n".format(total_max_duration)
        for duration in total_stat_csv_order:
            _period_data = total_stat_dict[duration]
            total_context += "{}, {}, {}\n".format(duration, _period_data, "%.2f" % (_period_data * 1.0 / total_count_duration))
        total_context += '\n\n'

        total_whole_context = total_context + total_whole_context

    result_file = os.path.join(directory_path, 'queryorinsert_result.csv')
    with open(result_file, 'w') as logfile_s_writter:
        logfile_s_writter.write(total_whole_context)


if __name__ == '__main__':
    main_queryinsert()
