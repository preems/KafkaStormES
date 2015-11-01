# KafkaStormES
This is a simple Apache Storm Trident topology reading from Kafka and indexing in Elastic Search. 

##Setup Required to Run this Storm Topology
###1) Zookeeper.
<pre>
Download from http://apache.osuosl.org/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
CD to zookeeper folder
$ cp conf/zoo_sample.cfg conf/zoo.cfg
$ bin/zkServer.sh start
</pre>
###2) Kafka. 
<pre>
Download from http://apache.mirrors.pair.com/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz 
CD to kafka folder
$ bin/kafka-server-start.sh config/server.properties
Create a topic
$ bin/kafka-topics.sh --topic exampletopic --create --zookeeper localhost:2181 --partitions 1 --replication-factor 1
Create a command line producer and leave the tab open
$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic exampletopic
</pre>
###3) ElasticSearch
<pre>
Download from https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.2.tar.gz
CD to ElasticSearch Folder
$ bin/elasticsearch
Install the head plugin
$ bin/plugin -install mobz/elasticsearch-head
Open http://localhost:9200/_plugin/head/ in browser.
</pre>
## Run this Topology
<pre>
1) Extract the folder.
2) Run ‘mvn package’ inside the folder.
3) Run  “ storm jar target/kafka-example-1.0-jar-with-dependencies.jar com.preems.kafkaexample.KafkaExampleTopology "
4) Enter some sentences into the command line producer. Check the logs printed by the storm topology and check the ElasticSearch page for the same messages.
</pre>
