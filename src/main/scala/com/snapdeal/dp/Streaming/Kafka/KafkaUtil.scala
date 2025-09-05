package com.snapdeal.dp.Streaming.Kafka

import com.snapdeal.dp.DPEvent.Event
import com.snapdeal.dp.Streaming.StreamUtil
import com.snapdeal.dp.constants.DpConstants
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties

/**
 * Created by Bhavya Joshi
 */

class KafkaUtil(props : Properties) {

  private val logger = LoggerFactory.getLogger(classOf[KafkaUtil])

  val getKafkaParams : Map[String,String] = {
    val config = Map(
      DpConstants.KAFKA_BROKERS     -> props.getProperty(DpConstants.KAFKA_BROKERS),
      DpConstants.SCHEMA_REGISTRY   -> props.getProperty(DpConstants.SCHEMA_REGISTRY),
      DpConstants.KAFKA_GROUP       -> props.getProperty(DpConstants.KAFKA_GROUP)
    )
    logger.info("Successfully fetched kafka params .......")
    config
  }

  def getKafkaConsumerConfig (dpEvent : Event) : Map[String,String] = {
    val config = Map(
      DpConstants.KAFKA_TOPIC       -> StreamUtil.getDPTopic(dpEvent),
      DpConstants.STARTING_OFFSETS  -> props.getProperty(DpConstants.STARTING_OFFSETS),
      DpConstants.FAIL_ON_DATA_LOSS -> props.getProperty(DpConstants.FAIL_ON_DATA_LOSS)
    )
    logger.info(s"Successfully fetched consumer Kafka config for event ${dpEvent.getEventKey}")
    config
  }
  val getAvroOptions : java.util.HashMap[String,String] = {
    val avroOption = new util.HashMap[String,String]()
      avroOption.put(DpConstants.AVRO_MODE,props.getProperty(DpConstants.AVRO_MODE))
    avroOption
  }

  def getLatestAvroSchema (dpEvent : Event) : String = {
    val topic = StreamUtil.getDPTopic(dpEvent)
    val subject  = s"${topic}-value"
    var schema : String  = null
    try {
      val client = new CachedSchemaRegistryClient(props.getProperty(DpConstants.SCHEMA_REGISTRY), 100)
      schema = client.getLatestSchemaMetadata(subject).getSchema
    }catch {
      case e : Exception => logger.info("Exception while parsing Avro Schema from schema registry ..... ")
        throw e
    }
    logger.info(s"Latest Schema fetched for ${topic} :  {}", schema )
    schema

  }


}
