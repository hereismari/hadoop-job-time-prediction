# Hadoop Job Time Prediction

Code used to perform some Hadoop job predictions experiments using OpenStack Sahara.

###### Cluster size details

The master and slaves of the clusters had a flavor with the following configurations:
  - 2 VCPUs
  - 45 GB HD
  - 4 GB RAM
  - 4 GB swap

## Running experiment

To run this experiment first you must:
  1. Get the input
  2. Compile classes in source folder and get the jar file. Or get the jar file avalaible in this repository.
  3. Put what is need in Sahara
  4. Make a Json configuration file with the same structure of "configuration_default.json"
  5. Let this awesome experiment running and go have some good time, it will email you when it's done ;)
  6. Generate graphs for a more visual result

#### 1. Getting the input file

Input file used in the experiment can be accessed at [this link](https://www.dropbox.com/s/1e4zdjq6l8tj7eb/5GB?dl=0). 

This file was generate by TeraSortGen of Hadoop 1.2.1 examples, doing the following command:

`bin/hadoop jar hadoop-examples-1.2.1.jar terasortgen 50000000`

If you want to generate the file yourself, you must:
- [Install hadoop 1.2.1](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html#Installing+Software)
- Unpack the downloaded Hadoop. Edit the file conf/hadoop-env.sh to define at least JAVA_HOME to be the root of your Java installation.
- Then run : `bin/hadoop jar hadoop-examples-1.2.1.jar terasortgen 50000000`
- In case of doubts: more information about TeraSortGen [here](http://www.michael-noll.com/blog/2011/04/09/benchmarking-and-stress-testing-an-hadoop-cluster-with-terasort-testdfsio-nnbench-mrbench/)

 
#### 2. Compile the classes and generate JAR file

To compile the classes you should:
  1. have hadoop 2.6.0 installed, you can have more information about how to do it [here](https://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
  2. Download the source folder, and put it in the same directory hadoop is installed(you can put the source folder somewhere else, but it makes it easier if everything is in the same place, and you can delete it when you're done if you don't want this in the hadoop folder)
  2. Once you got the source folder(and is in the same folder of hadoop), compile the classes with thess commands:
    ```
    $export JAVA_HOME=/usr/java/default
    $export PATH=${JAVA_HOME}/bin:${PATH}
    $export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
    
    $ bin/hadoop com.sun.tools.javac.Main source/*.java
    ```
  3. Now create the jar running:
    ```
    
    $jar cf experiment.jar source/*.class
    
    ```

If you have any doubts about topics 2 and 3 you can have more information about it [here](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html).

### 3. Put what is need in Sahara

  1. Create a key_pair, if you already have one you can use it (The local path of your public and private key will be needed) 
  2. You'll have to put the jar as a job binary in Sahara and create a job template of type JavaAction for each job(PiEstimator, TeraSort and WordCount). You can have acces to a similar proccess [here](https://docs.google.com/presentation/d/12X1dvb8tbSPfE1gdIqzU3X8ImdrnLtp_n1D9tK9H8NI/edit?usp=sharing).
  2. You'll have to create a master and woker node group template, and a cluster template with(3,4,5,...10) nodes.
      A similar proccess can be seen [here](https://docs.google.com/presentation/d/1VYBbipv8cgMvRWc7oFYSIMWLSkEPBO08_HJ1aMdeeOI/edit?usp=sharing).
  3. create a volume and put the 5GB file in it. You can contact me if you need help in this proccess, I plan to do some post about it, and when I do I'll put in here!

### 4. Change json configuration file
  You can get all these informations trough Horizon, except: public_keypair_path, private_keypair_path and private_keypair_name that only you have access.

### 5. Run, baby, run!

  Now should all be ready to run :heart:!
  You can run this experiment in 2 different ways:
  - Running 
    ```$python runExperiment.py  <number of executions> <configuration path> <output file name>```
    *With number of executions = 8*
  - Running 
    ```$python runExperimentIndividuall.py <number of executions> <number of cluster nodes> <configuration path>     <output file name>```
    *With number of executions = 8 and number of cluster = [3,10]*

### 6. Generate graphs

  Now that you have the ouput files, the final step is generate the graphs.
  If you used runExperimentIndividually.py you must concatenate all files in one, you can do that by:
  ``` $ cat <output_1_node> <output_2_nodes> <output_3_nodes> ... > output_exp ```
  
  ATENTION: before run scripts change the input_file and output_file names.
  Also check if files are in the same folder or change path in the beggining of the script with the command:
  ``` setwd("your_path") ```
  
  Then, go to *analysis* folder and do the following:
  1. Execute filtrate.R. It will generate a new file named as output_name.
  2. Run KNN.R in the file previously generated, and it will generate a new file.
  3. Run graph_cost.R and graph_prediction.R with the input = KNN.R output. They will generate graphs in pdf format.
    
  And now you have some awesome graphs :sunglasses: !!!
