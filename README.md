# PageRank-Algorithm
### Following commands are used for creating the below directory:

$ sudo su hdfs
$ hadoop fs -mkdir /user/rjain12/asgn3
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/rjain12/asgn3/input 


All the INPUT files ARE stored on hdfs in /user/cloudera/rjain12/input

#### Directories created for the PageRank programs:

1. /user/rjain12/asgn3/input
2. /user/rjain12/asgn3/output
4. /user/rjain12/asgn3/temp

Ranking.java is present in /home/cloudera/Desktop/Assignment3/Ranking.java


#### So, the following commands are used to compile the class:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* /home/cloudera/Desktop/Assignment3/Ranking.java -d build -Xlint 

Jar file is stored in /home/cloudera/ranking.jar
$ jar -cvf ranking.jar -C build/ . 

##### Running the application:
Give 3 arguments from the terminal
one for input, one for temp and one for the output path

$ hadoop jar ranking.jar org.myorg.Ranking /user/rjain12/asgn3/input /user/rjain12/asgn3/temp /user/rjain12/asgn3/output

##### Output is stored in /user/rjain12/asgn3/output
$ hadoop fs -cat /user/rjain12/asgn3/output/*

##### Saving the output file using below command:
$ hdfs dfs -get //user/rjain12/asgn3/output/part-r-00000 /home/cloudera/Desktop/output/
