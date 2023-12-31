## COMP-5012 "BIG DATA" Course Project

## 1. Setting up Kafka

Follow the tutorial here: https://kafka.apache.org/quickstart

To produce/consume to/from a topic, make sure the zookeeper and kafka servers are both running. 

Make sure the IP addresses in the server.properties and zookeeper.properties match to your local machine/cluster (will change depending on the network).


Troubleshooting:

If the zookeeper server does not run properly, try running: call kafka-run-class org.apache.zookeeper.server.quorum.QuorumPeerMain ..\..\config\zookeeper.properties

If the kafka server does not run with the .sh file, try running: kafka-server-start.bat ..\..\config\server.properties

In the producer python script, make sure the IP addresses in your bootstrap-server lists match the machines you want to publish to. 

For example, if you are just working on one machine, make sure you are only working with localhost:9092. 

If you want other machines to see what you publish on this topic, be sure to include their IP addresses in this list. For this, you also need to add the local machine's IP address to the advertised.listeners field in server.properties.




## 2. Connecting Spark

Add the path to your hadoop directory to your environment variables, under the name HADOOP_HOME.

Add %HADOOP_HOME%\bin to your Path environment variable.

Copy hadoop.dll from the hadoop/bin directory to windows/System 32

Be sure to have the zookeeper server and kafka server running. Make sure you have created a Kafka topic.

Start your pyspark consumer (it is subscribed to a topic so will act as a listener).

Publish to the topic using the Kafka producer.

You should see the data print in the consumer console.

## 3. Registering UDFs

In the example, there is a file called udf_functions.py that contains and registers a UDF. This can be used as a template for defining and registering your own UDF. ** NOTE THAT THIS FILE NO LONGER EXISTS AND THE FUNCTIONS HAVE BEEN MOVED TO SPARK_JOB.PY **

In the spark_job.py file, this is the consumer. I renamed it for reasons I will explain later on in this section. This consumer is updated to include the results from the UDF as applied to the incoming stream.

In order to run the spark_job.py file, there is a little bit of troubleshooting. For some reason, VSCode was blocking it for me but it works fine in the command line. 

Second, you may come across this error: Python not found ... ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 0) org.apache.spark.SparkException: Python worker failed to connect back.

To resolve this, you need to set your SPARK_HOME environment variable to the spark folder on your machine. You also need to set a new variable called PYSPARK_PYTHON and set the address to your system's python.exe file.

After this, you should be able to run spark_job.py and see 3 columns with information (after running the producer).

## 4. Submitting Spark Jobs

In order to submit a spark job to your own local machine, you should navigate to the directory of the spark job. In this case, it is Kafka_Python\Testing. I renamed yahoo_consumer to spark_job so it would be easier to follow.

After navigating there, you can run the following in your cmd: path/to/spark/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 path/to-spark/job

For me, the above command looks like this: C:\spark\spark\bin\spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\Users\lsikk\git\comp5012_course_project\Kafka_Python\Testing\spark_job.py

This will run it on your local machine. You will get a LOT of logging. In that logging, there will be a URL at port 4040 that you can follow to see the progression/status of the job.

It seems that to create a cluster, you simply need to submit jobs to the master's URL insead of "--master local[*]" in your submission. 

## 5. Adding workers and submitting spark jobs to a cluster

You can have one machine be a master and a slave at the same time. Or you can have one machine be a master and the other be a slave. It does not matter. The important thing to know is that all the machines you are using MUST have more than 8GB RAM or you will get the error: "Python worker exited unexpectedly". 

Mkae sure you have the zookeeper server and kafka server running (on all machines). 

To register a master, navigate to the bin folder of your spark application and run the following in the cmd: spark-class.cmd org.apache.spark.deploy.master.Master.

This will generate a URL for a webpage. At the top of the webpage, there will be a url with a port number. You will use this to connect the worker.

In a DIFFERENT cmd, navigate to the bin folder of your spark application and run the following in the cmd: spark-class.cmd org.apache.spark.deploy.worker.Worker url/on/master/ui.

Refresh the master UI and you should see a worker in the worker list. 

Now you can submit a spark job on the master machine, but make sure to connect it to the master URL. So instead of local[*] write spark://________:7077.

For this to work, python.exe should be found at the same location on all machines.

In the example, submit the spark job (make sure there are no errors). Then run the yahoo producer (be sure to change the IP addresses accordingly). 

In the latest push, I made it so that the output is written to a file in C://sparkoutputs. There should be a JSON file in there for every batch you run. The first one will be blank. The names will be really weird e.g., part-00000-aa5c00ca-2e7c-4aaf-b890-9b8e25db4a22-c000.json. 







## Quick commands for next launch:

Start ZooKeeper:

cd c:\spark\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties


Start Kafka:

cd c:\spark\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

Create new topic my-topic:
cd c:\spark\kafka
.\bin\windows\kafka-topics.bat --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


Produce to my-topic:
cd c:\spark\kafka
.\bin\windows\kafka-console-producer.bat --topic my-topic --bootstrap-server localhost:9092

Consumer from my-topic;

cd c:\spark\kafka
.\bin\windows\kafka-console-consumer.bat --topic my-topic --bootstrap-server localhost:9092 --from-beginning

Register master:
cd c:\spark\spark\bin
spark-class.cmd org.apache.spark.deploy.master.Master


Connect worker:
cd c:\spark\spark\bin
spark-class.cmd org.apache.spark.deploy.worker.Worker spark://169.254.232.170:7077



Submit spark job

cd C:\spark\spark\bin
spark-submit --master spark://192.168.1.118:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 C:\spark\project\Kafka_Python\Testing\spark_job.py




## Producing Data from One Device to Another:
Before starting Zookeeper and Kafka, navigate to the Kafka configuration directory on the consumer device. Edit the server.properties file to set the advertised.listeners property to the external IP address or hostname of the Kafka server on the producer device.

advertised.listeners=PLAINTEXT://<producer-device-ip>:9092

Perform this configuration on all devices, and don't forget to delete the '#' to uncomment the line.


Now, start Zookeeper and Kafka, create a topic, and produce the data on the producer device.

Consuming Data:

Open a new CMD window and navigate to the Kafka directory. Use the kafka-console-consumer.bat script to start consuming data from the topic:

bin\windows\kafka-console-consumer.bat --topic your_topic_name --bootstrap-server <producer-device-ip>:9092







