# spark-streaming-cassandra

This maven project reads data from Kafka topic using Spark Streaming and populate the data in Cassandra database.

###Technologies Used:
- Scala
- Java 8
- Spark Streaming
- Spark Cassandra Connector
- Kafka


###How to build
mvn clean install

###How to deploy
Using Spark Submit command you can run the jar file providing --master, --class and jar file location.
