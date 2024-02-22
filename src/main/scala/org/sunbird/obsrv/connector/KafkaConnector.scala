package org.sunbird.obsrv.connector

import com.typesafe.config.Config
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetResetStrategy}
import org.json.{JSONException, JSONObject}
import org.sunbird.obsrv.connector.source.{IConnectorSource, SourceConnector, SourceConnectorFunction}
import org.sunbird.obsrv.job.exception.UnsupportedDataFormatException
import org.sunbird.obsrv.job.model.Models.ErrorData

import java.nio.charset.StandardCharsets
import java.util.Properties

object KafkaConnector {

  def main(args: Array[String]): Unit = {
    SourceConnector.process(args, new KafkaConnectorSource, new KafkaConnectorFunction)
  }
}

class KafkaConnectorSource extends IConnectorSource {

  private def kafkaConsumerProperties(config: Config): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.getString("source.kafka.consumer.broker-servers"))
    properties.setProperty("group.id", config.getString("source.kafka.consumer-id"))
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties.setProperty("auto.offset.reset", config.getString("source.kafka.auto-offset-reset"))
    properties
  }

  private def kafkaSource(config: Config): KafkaSource[String] = {
    val dataFormat = config.getString("source.data-format")
    if(!"json".equals(config.getString("source.data-format"))) {
      throw new UnsupportedDataFormatException(dataFormat)
    }
    KafkaSource.builder[String]()
      .setTopics(config.getString("source.kafka.topic"))
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(kafkaConsumerProperties(config))
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  override def getSourceStream(env: StreamExecutionEnvironment, config: Config): SingleOutputStreamOperator[String] = {
    env.fromSource(kafkaSource(config), WatermarkStrategy.noWatermarks[String](), config.getString("source.kafka.consumer-id")).uid(config.getString("source.kafka.consumer-id"))
  }
}

class KafkaConnectorFunction extends SourceConnectorFunction {
  override def processEvent(event: String, onSuccess: String => Unit, onFailure: (String, ErrorData) => Unit): Unit = {

    if (event == null) {
      onFailure(event, ErrorData("EMPTY_JSON_EVENT", "Event data is null or empty"))
    } else if (!isValidJSON(event)) {
      onFailure(event, ErrorData("JSON_FORMAT_ERR", "Not a valid json"))
    } else {
      onSuccess(event)
    }
  }

  private def isValidJSON(json: String): Boolean = {
    try {
      new JSONObject(json)
      true
    }
    catch {
      case _: JSONException =>
        false
    }
  }
}

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {

  private val serialVersionUID = -3224825136576915426L

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    out.collect(new String(record.value(), StandardCharsets.UTF_8))
  }
}