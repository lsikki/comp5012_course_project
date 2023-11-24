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

In the example, there is a file called udf_functions.py that contains and registers a UDF. This can be used as a template for defining and registering your own UDF.

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

It seems that to create a cluster, you simply need to submit jobs to the master's URL insead of "--master local[*]" in your submission. More to come on this soon!
