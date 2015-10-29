from utils.openstack.UtilSwift import *
from utils.openstack.ConnectionGetter import *
from utils.openstack.UtilSahara import *
from utils.openstack.UtilKeystone import *
from utils.openstack.UtilNova import *
from utils.experiment.JsonParser import *
from utils.experiment.sendMail import sendMail

import os
import sys
import subprocess
import time
import getpass
from subprocess import Popen, call, PIPE

#--------------------- DEFAULT CONFIGURATIONS -------------------

MIN_NUM_ARGS = 3
DEF_CLUSTER_NAME = "exp-hadoop-"
HDFS_BASE_DIR = "/user/hadoop/"
HOME_INSTANCE_DIR = "/home/hadoop"
DEF_INPUT_DIR = "input"
DEF_INPUT_SIZE_MB = 5059

#-------------------- FUNCTIONS ----------------------------------

def configureInstances(instancesIps, publicKeyPairPath, privateKeyPairPath):
	print "Configuring Instances..."
	for instanceIp in instancesIps:
        	commandArray = ["./SSHWithoutPassword.sh", instanceIp , publicKeyPairPath, privateKeyPairPath] 
        	command = ' '.join(commandArray)
        	print command
        	call(command,shell=True)
        	print "Configured instance with IP:", instanceIp

def copyFileToInstances(filePath,instancesIps,keypairPath):
    print "Copying " + filePath +" file to instances"
    for instanceIp in instancesIps:
        commandArray = ["scp","-i",keypairPath,"-r",filePath,"hadoop@"+instanceIp+ ':'+HOME_INSTANCE_DIR]
        command = ' '.join(commandArray)
        print command
        call(command,shell=True)
        print "Copied file to instance with IP:", instanceIp

def putFileInHDFS(filePath, masterIp,keypairPath):
    connection['nova'].attach_volume(server_id, volume_id)
    time.sleep(2)
    file_name = filePath.split('/')[-1]
    print file_name
    remoteFilePath = file_name
    commandArray = ["ssh -i",keypairPath,"hadoop@" + masterIp, "'cat | python -", remoteFilePath, DEF_INPUT_DIR + "'", "<", "./putFileInHDFS.py"]
    command = ' '.join(commandArray)
    print command
    call(command,shell=True)
    connection['nova'].detache_volume(server_id, volume_id)

    print "Success! File is now at HDFS of cluster!"

def deleteHDFSFolder(keypairPath, masterIp):
	commandArray = ["ssh -i",keypairPath,"hadoop@" + masterIp, "'cat | python - '", "<", "./removeOutputFile.py"]
	command = ' '.join(commandArray)
	print command
	call(command,shell=True)

def saveJobResult(job_res, job_name, cluster_size, master_ip, num_reduces, job_num, output_file, input_size):
	result = ";".join((job_name, str(num_reduces), str(input_size), str(cluster_size), str(job_res['time']), job_res['status'])) + "\n"
	print result
	print "Finished"
	f = open(output_file, 'ab')
	f.write(result)
	f.close()

def getConnection(user, password, project_name, project_id, main_ip):

    result = {}

    connector = ConnectionGetter(user, password, project_name, project_id, main_ip)

    keystone = UtilKeystone(connector.keystone())
    token_ref_id = keystone.getTokenRef(user, password, project_name).id

    sahara = UtilSahara(connector.sahara(token_ref_id))

    nova = UtilNova(connector.nova())

    result['keystone'] = keystone
    result['sahara'] = sahara
    result['nova'] = nova

    return result

def printUsage():
	print "python runExperiment.py <numberExecs> <configFilePath> <outputFile>"

if (len(sys.argv) < MIN_NUM_ARGS):
        print "Wrong number of arguments: ", len(sys.argv)
        printUsage()
        exit(1)

#------------ CONFIGURATIONS -----------------
number_execs = int(sys.argv[1])
config_file_path = sys.argv[2]
output_file = sys.argv[3]

user = raw_input('OpenStack User: ')
password = getpass.getpass(prompt='OpenStack Password: ')

gmail_user = raw_input('Gmail User(without @gmail.com): ')
gmail_password = getpass.getpass(prompt='Gmail Password: ')

json_parser = JsonParser(config_file_path)

main_ip = json_parser.get('main_ip')

project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')

public_keypair_path = json_parser.get('public_keypair_path')
private_keypair_path = json_parser.get('private_keypair_path')
private_keypair_name = json_parser.get('private_keypair_name')
input_file_path = json_parser.get('input_file_path')

net_id = json_parser.get('net_id')
image_id = json_parser.get('image_id')
volume_id = json_parser.get('volume_id')

mapred_factors = json_parser.get('mapred_factor')

#------------ GETTING CONNECTION WITH OPENSTACK -----------------
connection = getConnection(user, password, project_name, project_id, main_ip)

#----------------------- EXECUTING EXPERIMENT ------------------------------
job_number = 0
for cluster_template in json_parser.get('cluster_templates'):
	
	cluster_template_id = cluster_template['id']
	cluster_size = cluster_template['n_slaves']
	cluster_name = DEF_CLUSTER_NAME + '-' +  str(cluster_size)

	print 'Running experiment for cluster with number of workers =', cluster_size
	######### CREATING CLUSTER #############
	try:
	    cluster_id = connection['sahara'].createClusterHadoop(cluster_name, image_id, cluster_template_id, net_id, private_keypair_name)
	except RuntimeError as err:
		print err.args
		break
		
	######### CONFIGURING CLUSTER ##########
	instancesIps = connection['sahara'].get_instances_ips(cluster_id)
	configureInstances(instancesIps, public_keypair_path, private_keypair_path)
	master_ip = connection['sahara'].get_master_ip(cluster_id)
	server_id = connection['sahara'].get_master_id(cluster_id)
	putFileInHDFS(input_file_path, master_ip, private_keypair_path)

	flag_terasort = True #TeraSort must run only one time

	for mapred_factor in mapred_factors:
		
		mapred_reduce_tasks = str(int(round(2*(mapred_factor)*cluster_size))) # 2 == mapred.tascktracker.reduce.maximum default value

		for job in json_parser.get('jobs'):

			 if job['name'] == 'TeraSort' and flag_terasort:
                    flag_terasort = False
                    continue

			######### RUNNING JOB ##########
			numFailedJobs = 0
			numSucceededJobs = 0
			while numSucceededJobs < number_execs:
				try:    
					input_size = DEF_INPUT_SIZE_MB
					num_reduces = mapred_reduce_tasks

					if job['name'] == 'TeraSort':
						num_reduces = '1'

					job_res = connection['sahara'].runJavaActionJob(main_class=job['main_class'], job_id=job['template_id'], cluster_id=cluster_id, reduces=num_reduces, args=job['args'])
				
					if job['name'] == 'Pi Estimator':
						input_size = job['args'][0]
					
					saveJobResult(job_res, job['name'], cluster_size, master_ip, num_reduces, job_number, output_file, input_size)
					if (job_res['status'] != 'SUCCEEDED'):
						numFailedJobs += 1
					else:
						numSucceededJobs += 1
					
					deleteHDFSFolder(private_keypair_path,master_ip)
					print "Break time... go take a coffee and relax!"
					time.sleep(5)
			
				except Exception, e:
					print "Exception: ", e
					connection = getConnection(user, password, project_name, project_id, main_ip)
					deleteHDFSFolder(private_keypair_path,master_ip)
	
			sendMail('Finished job %s\nsucces_jobs:%s failed_jobs:%s reduce fator %s' % (job['name'], numSucceededJobs, numFailedJobs, str(mapred_factor)) , gmail_user + '@gmail.com', gmail_user + '@gmail.com', gmail_password, cluster_size, output_file)
	connection['sahara'].deleteCluster(cluster_id)
	print 'FINISHED FOR CLUSTER ' + cluster_name
	sendMail('Success!', gmail_user + '@gmail.com', gmail_user + '@gmail.com', gmail_password, cluster_size, output_file)
