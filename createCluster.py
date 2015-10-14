from utils.openstack.UtilSwift import *
from utils.openstack.ConnectionGetter import *
from utils.openstack.UtilSahara import *
from utils.openstack.UtilKeystone import *
from utils.experiment.JsonParser import *
import os
import sys
import subprocess
import time
import getpass

MIN_NUM_ARGS = 4

def printUsage():
    print "python create_cluster.py <cluster_name> <key_pair> <config_file_path>"

if (len(sys.argv) < MIN_NUM_ARGS):
    print "Wrong number of arguments"
    printUsage()
    exit(1)

cluster_name = sys.argv[1]
key_pair = sys.argv[2]
config_file_path = sys.argv[3]

user = raw_input('OpenStack User: ')
key = getpass.getpass(prompt='OpenStack Password: ')
json_parser = JsonParser(config_file_path)

project_name = json_parser.get('project_name')
project_id = json_parser.get('project_id')
main_ip = json_parser.get('main_ip')
connector = ConnectionGetter(user, key, project_name, project_id, main_ip)

keystone_util = UtilKeystone(connector.keystone())
token_ref_id = keystone_util.getTokenRef(user, key, project_name).id
sahara_util = UtilSahara(connector.sahara(token_ref_id))

cluster_id = sahara_util.createClusterHadoop(cluster_name, image_id, template_id, net_id,key_pair)
print cluster_id

