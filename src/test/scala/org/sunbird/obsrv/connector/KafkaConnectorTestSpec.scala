package org.sunbird.obsrv.connector

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.Matchers._
import org.sunbird.obsrv.connector.source.IConnectorSource
import scala.collection.JavaConverters._

class KafkaConnectorTestSpec extends BaseFlinkConnectorSpec with Serializable {

  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-connector-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )

  override def beforeAll(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka("test-kafka-topic", EventFixture.INVALID_JSON)
    EmbeddedKafka.publishStringMessageToKafka("test-kafka-topic", EventFixture.VALID_KAFKA_EVENT)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List("sb-knowlg-topic").foreach(EmbeddedKafka.createCustomTopic(_))
  }

  override def getConnectorName(): String = "KafkaConnector"

  override def getConnectorSource(): IConnectorSource = new KafkaConnectorSource()

  override def testFailedEvents(events: java.util.List[String]): Unit = {
    events.size() should be (1)
    events.asScala.head should be ("""{"name":"/v1/sys/health","context":{"trace_id":"7bba9f33312b3dbb8b2c2c62bb7abe2d"""")
  }

  override def testSuccessEvents(events: java.util.List[String]): Unit = {
    events.size() should be (1)
  }

  override def getConnectorConfigFile(): String = "test-config.json"

  override def getSourceConfig(): Map[String, AnyRef] = {
    Map(
      "source_kafka_broker_servers" -> "localhost:9093",
      "source_kafka_consumer_id" -> "kafka-connector",
      "source_kafka_auto_offset_reset" -> "earliest",
      "source_data_format" -> "json",
      "source_kafka_topic" -> "test-kafka-topic",
    )
  }

}