import subprocess
import os
import time
import sys

MIN_NUM_ARGS = 3

def printUsage():
        print "python PutFileInHDFS.py <filePath> <dirName>"

def runHDFSCommand(args):
    command = ["/opt/hadoop/bin/hdfs", "dfs"]
    command += args.split()
	
    print command
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait()
    return (proc.communicate(),proc.returncode)

def createHDFSDir(dirPath):
    runHDFSCommand("-mkdir -p " + dirPath)

def putFileInHDFS(filePath, destPath="", blockSize=None):
    if blockSize != None:
        runHDFSCommand("-Ddfs.block.size=" + blockSize + " -put " + filePath + " " + destPath)
    else:
        runHDFSCommand("-put " + filePath + " " + destPath)


if (len(sys.argv) < MIN_NUM_ARGS):
    print "Wrong number of arguments: ", len(sys.argv)
    printUsage()
    exit(1)

file_path = sys.argv[1]
dir_name = sys.argv[2]

createHDFSDir(dir_name)
putFileInHDFS(file_path, dir_name)
