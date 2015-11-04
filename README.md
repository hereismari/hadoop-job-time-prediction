# Hadoop Job Time Prediction
Code used to perform some Hadoop job predictions experiments using OpenStack Sahara.

## Input File

Input file used in the experiment can be accessed at [this link](https://www.dropbox.com/s/1e4zdjq6l8tj7eb/5GB?dl=0). 
This file was generate by TeraSortGen of Hadoop 1.2.1 examples, doing the following command:

`bin/hadoop jar hadoop-examples-1.2.1.jar terasortgen 50000000`

In order to generate this file you must:
- [Install hadoop 1.2.1](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html#Installing+Software)
- In case of doubts: more information about TeraSortGen [here](http://www.michael-noll.com/blog/2011/04/09/benchmarking-and-stress-testing-an-hadoop-cluster-with-terasort-testdfsio-nnbench-mrbench/)
