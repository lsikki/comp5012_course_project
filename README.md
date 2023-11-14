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
