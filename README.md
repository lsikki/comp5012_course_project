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
