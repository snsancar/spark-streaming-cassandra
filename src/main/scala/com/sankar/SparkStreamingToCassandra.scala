package com.sankar

package com.sankar

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns
import java.io._
import com.datastax.spark.connector.toNamedColumnRef
import scala.reflect.runtime.universe

/**
 * Class used to consume XML data from Kafka topic using Spark Streaming and populate the data in Cassandra.
 */
object KafkasSparkCassandaraXML {

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: KafkaStreaming <zkQuorum> <group> <topics> ")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics) = args
    val sparkConf = new SparkConf().setAppName("KafkaSparkStreaming").set("spark.cassandra.connection.host", "10.251.8.202")
    val ssc = new StreamingContext(sparkConf, Seconds(20))

    val topicpMap = topics.split(",").map((_, 1.toInt)).toMap
    val inputDStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    val xmlDStream = inputDStream.map(record => record.toString())
    val userDStream = xmlDStream.map(x => {

      (extractField(x, "custid"), extractField(x, "custname"), extractField(x, "paymentid"), extractField(x, "trnsamt"), extractField(x, "trnsdate"))
    })
    userDStream.saveToCassandra("db", "payment", SomeColumns("custid", "custname", "paymentid", "trnsamt", "trnsdate"))
    ssc.start
    ssc.awaitTermination
  }

  /**
   * Method used to extract the specific element from the XML data.
   */
  def extractField(tuple: String, tag: String) = {
    var value = tuple
    if (value.contains("<" + tag + ">") && value.contains("</" + tag + ">")) {
      value = value.split("<" + tag + ">")(1).split("</" + tag + ">")(0)
    }
    value
  }
}