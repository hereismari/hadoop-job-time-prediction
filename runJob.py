from classes.UtilSwift import *
from classes.ConnectionGetter import *
from classes.UtilSahara import *
from classes.UtilKeystone import *
from utils.experiment.JsonParser import *
import os
import sys
import subprocess
import time
import getpass
from subprocess import Popen, call, PIPE

MIN_NUM_ARGS = 7

def printUsage():
	print "python run_streaming_job.py <inputDataSourceId> <clusterId> <masterIpAddress> <mapperExecCmd> <reducerExecCmd> <numReduceTasks>"

if (len(sys.argv) < MIN_NUM_ARGS):
	print "Wrong number of arguments: ", len(sys.argv)
	printUsage()
	exit(1)

#------------ CONFIGURATIONS -----------------

json_parser = JsonParser(config_file_path)
execLocalPath = json_parser.get('exec_local_path')
publicKeyPath = json_parser.get('public_key_path')
keypairPath = json_parser.get('private_keypair_path')
project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')
main_ip = json_parser.get('main_ip')
output_container_name = json_parser.get('output_container_name')

password = ''

input_ds_id = sys.argv[1]
cluster_id = sys.argv[2]
master_ip = sys.argv[3]
mapperExecCmd = sys.argv[4]
reducerExecCmd = sys.argv[5]
mapred_reduce_tasks = sys.argv[6]

exec_date = datetime.now().strftime('%Y%m%d_%H%M%S')
output_ds_name ="output_%s_exp_%s_%s" % (username,mapred_reduce_tasks, exec_date)

#----------------------- GETTING CONNECTION ------------------------------
connector = ConnectionGetter(user, key, project_name, project_id, main_ip)

keystone_util = UtilKeystone(connector.keystone())
token_ref_id = keystone_util.getTokenRef(user, key, project_name).id
sahara_util = UtilSahara(connector.sahara(token_ref_id))

#----------------------- CREATING DATASOURCES ------------------------------
container_url = "swift://%s.sahara/%s" % (container_out_name, output_ds_name)
print "Sahara Container URL: %s" % container_url

data_source_out = sahara.data_sources.create("exp_"+output_ds_name,
                                         "Experiment",
                                         "swift",
                                         container_url,
                                         credential_user=username,
                                         credential_pass=password)

output_ds_id = data_source_out.id

#----------------------- RUNNING JOB ------------------------------
sahara_util.runJavaActionJob(main_class, job_id, cluster_id, reduces=map_reduce_tasks, input_ds_id =input_ds_id, output_ds_id=output_ds_id)

#----------------------- GETTING RESULTS ------------------------------
result = ";".join((job_name, str(maps), str(reduces), str(input_size), str(cluster_size), str(total_time),job_status))
print result
print "Finished"
