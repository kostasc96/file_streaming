package app

import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.StringSerializer

case class KafkaProducerHandler(server: String, flushInterval: Long = 100L) {

  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put("acks", "all")
  props.put("retries", "5")
  props.put("linger.ms", "50")
  props.put("batch.size", "16384")

  private val producer = new KafkaProducer[String, String](props)
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // Scheduled executor to flush the producer periodically
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  scheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      producer.flush()
    }
  }, flushInterval, flushInterval, TimeUnit.MILLISECONDS)

  /**
   * Sends a message to the given topic.
   * Optionally, a specific partition can be provided.
   */
  def send(topic: String, message: Any, partition: Option[Int] = None): Unit = {
  val messageStr = mapper.writeValueAsString(message)
    val record = partition match {
      case Some(part) => new ProducerRecord[String, String](topic, part, null, messageStr)
      case None       => new ProducerRecord[String, String](topic, messageStr)
    }
    producer.send(record)
  }

  /**
   * Closes the producer and shuts down the flush scheduler.
   */
  def close(): Unit = {
    scheduler.shutdown()
    try {
      scheduler.awaitTermination(flushInterval, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException => e.printStackTrace()
    }
    producer.flush()
    producer.close()
  }
}
