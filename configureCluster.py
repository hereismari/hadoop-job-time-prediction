from datetime import datetime
import random
import requests
import time
import sys
import getpass
from utils.openstack.UtilSwift import *
from utils.openstack.ConnectionGetter import *
from utils.openstack.UtilSahara import *
from utils.openstack.UtilKeystone import *
from utils.experiment.JsonParser import *
from subprocess import Popen, call, PIPE

MIN_NUM_ARGS = 3

def printUsage():
	print "python configureCluster.py <clusterId> <config_file_path>"

def configureInstances(instancesIps):
	print "Configuring Instances..."
	for instanceIp in instancesIps:
        	commandArray = ["./SSHWithoutPassword.sh",instanceIp,publicKeyPath,keypairPath] 
        	command = ' '.join(commandArray)
        	print command
        	call(command,shell=True)
        	print "Configured instance with IP:", instanceIp

def copyExecFileToInstances(execFilePath,instancesIps):
	print "Copying exec file to instances"
	for instanceIp in instancesIps:
        	commandArray = ["scp","-i",keypairPath,"-r",execLocalPath,"hadoop@"+instanceIp+DEF_EXEC_INSTANCE_PATH]   
 		command = ' '.join(commandArray)
        	print command
        	call(command,shell=True)
		print "Copied exec file to instance with IP:", instanceIp



if (len(sys.argv) < MIN_NUM_ARGS):
	print "Wrong number of arguments: ", len(sys.argv)
	printUsage()
	exit(1)


clusterId = sys.argv[1]
config_file_path = sys.argv[2]

user = raw_input('OpenStack User: ')
key = getpass.getpass(prompt='OpenStack Password: ')

json_parser = JsonParser(config_file_path)
execLocalPath = json_parser.get('exec_local_path')
publicKeyPath = json_parser.get('public_key_path')
keypairPath = json_parser.get('private_keypair_path')
project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')
main_ip = json_parser.get('main_ip')

DEF_EXEC_INSTANCE_PATH = ":/home/hadoop"

connector = ConnectionGetter(user, key, project_name, project_id, main_ip)

keystone_util = UtilKeystone(connector.keystone())
token_ref_id = keystone_util.getTokenRef(user, key, project_name).id
sahara_util = UtilSahara(connector.sahara(token_ref_id))

instancesIps = sahara_util.get_instances_ips(clusterId)

configureInstances(instancesIps)
copyExecFileToInstances(execLocalPath,instancesIps)	

