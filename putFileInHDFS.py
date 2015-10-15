import subprocess
import os
import time
from LinuxUtils import LinuxUtils

MIN_NUM_ARGS = 3

def printUsage():
        print "python PutFileInHDFS.py <filePath> <dirName>"

if (len(sys.argv) < MIN_NUM_ARGS):
    print "Wrong number of arguments: ", len(sys.argv)
    printUsage()
    exit(1)

file_path = sys.argv[1]
dir_name = sys.argv[2]

HadoopUtils.createHDFSDir(dir_name)
HadoopUtils.putFileInHDFS(file_path, dir_name)
