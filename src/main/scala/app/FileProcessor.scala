package app

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import com.google.flatbuffers.FlatBufferBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.io.FileNotFoundException
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Paths
import java.util.Properties
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object FileProcessor extends App {

  implicit val system: ActorSystem = ActorSystem("akka-streams")
  implicit val materializer: Materializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Configure Redis Jedis Pool
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(10)
  val jedisPool = new JedisPool(poolConfig, "localhost", 6379, 5000, null, false)

  val kafkaProps = new Properties()
  kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  val kafkaProducer = new KafkaProducer[String, Array[Byte]](kafkaProps)

  val resourceStream = getClass.getResourceAsStream("/mnist.csv")
  if (resourceStream == null) {
    throw new FileNotFoundException("File not found in resources!")
  }

  val resourcePath = Paths.get(getClass.getResource("/mnist.csv").toURI)
  val delimiter = ","
  val parallelism = 4

  def doubleArrayToBytes(data: Array[Double]): Array[Byte] = {
    val byteBuffer = ByteBuffer.allocate(data.length * java.lang.Double.BYTES)
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN) // Set to little-endian for NumPy compatibility
    data.foreach(byteBuffer.putDouble)
    byteBuffer.array()
  }

  val source = FileIO.fromPath(resourcePath)
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 65536, allowTruncation = true))
    .map(_.utf8String)
    .drop(1) // Skip the header
    .take(10) // Process only the first 10 rows for testing
    .zipWithIndex

  val processFlow = Flow[(String, Long)].mapAsync(parallelism) {
    case (line, index) =>
      Future {
        val columns = line.split(delimiter).map(_.toDouble)
        val label = columns.last.toInt.toString
        val data = columns.slice(0, columns.length - 1)

        // Convert data to byte array
        val byteData = doubleArrayToBytes(data)

        // Use Redis connection from the pool
        val redis = jedisPool.getResource
        try {
          redis.set(s"initial_data:$index".getBytes, byteData)
          redis.hset("images_label", index.toString, label)
        } finally {
          redis.close() // Always close the Redis connection
        }

        // Prepare and send message to Kafka specifying partition 0
//        val kafkaMessage = s"""{"layer": "layer_0", "image_id": "$index"}"""
        // Build the FlatBuffers message for activation using LayerMessage.
        // This replaces your JSON message.
        val builder = new FlatBufferBuilder(1024)
        // Create the string field for layer name.
        val layerString = builder.createString("layer_0")
        // Start building the LayerMessage.
        LayerMessage.startLayerMessage(builder)
        LayerMessage.addLayer(builder, layerString)
        LayerMessage.addImageId(builder, index.toInt)
        val messageOffset = LayerMessage.endLayerMessage(builder)
        builder.finish(messageOffset)
        // Retrieve the binary array.
        val buf: Array[Byte] = builder.sizedByteArray()
//        kafkaProducer.send(new ProducerRecord[String, String]("activate-layer", 0, null, kafkaMessage))
        kafkaProducer.send(new ProducerRecord[String, Array[Byte]]("activate-layer", 0, null, buf))

        // Introduce a sleep of 0.5 seconds between sends
        Thread.sleep(500)
      }
  }

  val result = source.via(processFlow).runWith(Sink.ignore)

  result.onComplete {
    case Success(_) =>
      println("Processing completed successfully.")
      jedisPool.close()
      kafkaProducer.close()
      system.terminate()

    case Failure(exception) =>
      println(s"Processing failed with exception: $exception")
      jedisPool.close()
      kafkaProducer.close()
      system.terminate()
  }
}
