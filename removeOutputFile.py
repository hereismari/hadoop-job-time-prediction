import subprocess
import os
import time
import sys

def runHDFSCommand(args):
    command = ["/opt/hadoop/bin/hdfs", "dfs"]
    command += args.split()

    print command
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait()
    return (proc.communicate(),proc.returncode)

def deleteOutputFile():
	command = '-rm -r output*'
	runHDFSCommand(command)
	command = '-rm -r PiEstimator_TMP_3_141592654'
	runHDFSCommand(command)

deleteOutputFile()
