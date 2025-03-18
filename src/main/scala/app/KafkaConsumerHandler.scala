package app

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class KafkaConsumerHandler(topic: String, servers: String, partition: Option[Int] = None) {

  private val props = new Properties()
  props.put("bootstrap.servers", servers)
  // You can set a group id if needed:
  // props.put("group.id", "your_group_id")
  props.put("auto.offset.reset", "latest")
  props.put("enable.auto.commit", "false")
  // Additional configurations as needed

  private val consumer = new KafkaConsumer[String, String](props)
  // JSON deserializer
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  partition match {
    case Some(part) =>
      val tp = new TopicPartition(topic, part)
      consumer.assign(java.util.Arrays.asList(tp))
    case None =>
      consumer.subscribe(java.util.Arrays.asList(topic))
  }

  /** Returns an iterator over Kafka messages */
  def consume(): Iterator[ConsumerRecord[String, String]] = {
    new Iterator[ConsumerRecord[String, String]] {
      private var recordsIterator: Iterator[ConsumerRecord[String, String]] = Iterator.empty
      override def hasNext: Boolean = {
        if (!recordsIterator.hasNext) {
          val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(500))
          recordsIterator = records.iterator().asScala
        }
        recordsIterator.hasNext
      }
      override def next(): ConsumerRecord[String, String] = {
        if (hasNext) recordsIterator.next()
        else throw new NoSuchElementException("No more records")
      }
    }
  }

  /** Polls messages for the given duration (in milliseconds) */
  def poll(durationMillis: Long = 500): ConsumerRecords[String, String] = {
    consumer.poll(Duration.ofMillis(durationMillis))
  }

  /** Commits a specific offset for the given topic and partition */
  def commit(topic: String, partition: Int, offset: Long): Unit = {
    val tp = new TopicPartition(topic, partition)
    val offsetData = new OffsetAndMetadata(offset)
    consumer.commitSync(java.util.Collections.singletonMap(tp, offsetData))
  }

  /** Closes the consumer */
  def close(): Unit = {
    consumer.close()
  }

  /** Example helper to deserialize JSON strings using Jackson */
  def deserializeJson(message: String): Any = {
    mapper.readValue(message, classOf[Object])
  }
}
