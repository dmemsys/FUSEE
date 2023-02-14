from genericpath import isdir
import json
import os

def get_workload_names(path):
    workload_name_list = []
    for i in os.listdir(path):
        file_path = os.path.join(path, i)
        assert(os.path.isdir(file_path) == False)
        print(file_path)
        if "upd" in file_path:
            workload_name_list.append(file_path)
    return workload_name_list

def mv_files(s_name, d_name):
    cmd = "mv {} {}".format(s_name, d_name)
    os.system(cmd)

def gen_workloads(workload_name_list):
    cmd = "./ycsb-c/ycsbc -db basic_file -threads 1 -P {}"
    for workload in workload_name_list:
        run_cmd = cmd.format(workload)
        print("Running: {}".format(run_cmd))
        os.system(run_cmd)
        
        # copy generated files
        d_name = workload.split("/")[-1] + "_load"
        mv_files("workload_load", "./upd-workloads/{}".format(d_name))
        d_name = workload.split("/")[-1] + "_trans"
        mv_files("workload_trans", "./upd-workloads/{}".format(d_name))

wllist = get_workload_names("./ycsb-c/workloads")
gen_workloads(wllist)
