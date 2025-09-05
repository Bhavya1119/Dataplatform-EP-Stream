package com.snapdeal.dp.Streaming

import com.snapdeal.dataplatform.admin.base.utils.FileUtils
import com.snapdeal.dp.DPEvent.Event
import com.snapdeal.dp.Streaming.Kafka.KafkaUtil
import com.snapdeal.dp.constants.DpConstants
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.DataTypes
import org.slf4j.LoggerFactory

import java.util.Properties


/**
 * Created by Bhavya Joshi
 */

object StreamUtil {

  private val logger =  LoggerFactory.getLogger(StreamUtil.getClass)
  private var avroOptions : java.util.HashMap[String,String] =  _
  private var kafkaProps = Map[String,String]()
  @volatile private var props: Properties = _
  private var dpkafkaUtils : KafkaUtil = _

  def init() : Unit = {
    props = FileUtils.loadPropertiesFromClassPath("app.properties")
    dpkafkaUtils = new KafkaUtil(props)
    kafkaProps = dpkafkaUtils.getKafkaParams
    avroOptions = dpkafkaUtils.getAvroOptions
    logger.info("Initialized context properties ...... ")
  }


  def getKafkaDF( spark : SparkSession, dpEvent : Event) : DataFrame = {

    val epConsumerProps = dpkafkaUtils.getKafkaConsumerConfig(dpEvent)
    val mergedKafkaProps = kafkaProps ++ epConsumerProps
    logger.info(s"Merged Kafka Config for consumer ${dpEvent.getEventKey}")
    val kafkaDF = spark.readStream.format(props.getProperty(DpConstants.STREAM_FORMAT))
    mergedKafkaProps.map(kv => kafkaDF.option(kv._1,kv._2))
    kafkaDF.load()
  }


  def getSparkSession() : SparkSession = {
    val conf = new SparkConf().setAppName("EP").setMaster("local[*]")
      .set(DpConstants.ACCESS_KEY,props.getProperty(DpConstants.ACCESS_KEY))
      .set(DpConstants.SECRET_KEY,props.getProperty(DpConstants.SECRET_KEY))
      .set(DpConstants.S3_IMPL,props.getProperty(DpConstants.S3_IMPL))
      .set(DpConstants.S3_ENDPOINT,props.getProperty(DpConstants.S3_ENDPOINT))
      .set(DpConstants.S3_CREDENTIAL_PROVIDER,props.getProperty(DpConstants.S3_CREDENTIAL_PROVIDER))
      .set(DpConstants.SPARK_CATALOG,props.getProperty(DpConstants.SPARK_CATALOG))
      .set(DpConstants.DELTA_SQL_EXTENSION,props.getProperty(DpConstants.DELTA_SQL_EXTENSION))

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

  def streamKafka(dpEvent: Event): StreamingQuery = {
    val dpAvroSchema = dpkafkaUtils.getLatestAvroSchema(dpEvent)
    try {
      val spark = SparkSession.getActiveSession.get

      val streamDF = getKafkaDF(spark, dpEvent)

      val sparkDF = extractPayload(dpAvroSchema, streamDF)

      val transformedDF = tranformDPStream(sparkDF)

      writeDPStream(transformedDF, dpEvent)

    } catch {

      case e1: SparkException =>
        logger.error("Exception while fetching active spark session ......")
        throw e1

      case e: Exception =>
        logger.error(s"Error in streaming: ${e.getMessage}", e)
        throw e

    }
  }

  def extractPayload(avroSchema : String ,df: DataFrame): DataFrame = {
    try{
      logger.info("Extracting Avro Values from Kafka Record ........")
     val structuredDF =  df.selectExpr("SUBSTRING(value,6) AS avro_value")
        .select(from_avro(col("avro_value"), avroSchema, avroOptions))
        .withColumnRenamed("from_avro(avro_value)","output")
        .select("output.*")
    structuredDF
    }catch {
      case e : Exception => logger.info("Exception while extracting avro value from kafka record")
        throw e
    }
  }

  @volatile
  def writeDPStream (transformedDF : DataFrame, dpEvent : Event): StreamingQuery = {

    try{
      val event = dpEvent.getEventKey
      logger.info("dpEvent in write 108 : {} ", event)
      logger.info(s"Props output path : ${props.getProperty(DpConstants.DELTA_OUTPUT_PATH)}")
      logger.info(s"checkpoint output path : ${props.getProperty(DpConstants.DELTA_CHECKPOINT_PATH)}")

      val outputPath = props.getProperty(DpConstants.DELTA_OUTPUT_PATH)
      val checkpointPath = props.getProperty(DpConstants.DELTA_CHECKPOINT_PATH)

      val fullOutputPath = s"$outputPath/$event"
      val fullCheckpointPath = s"$checkpointPath/$event"

      logger.info(s"Using output path: $fullOutputPath")
      logger.info(s"Using checkpoint path: $checkpointPath")

      val writerQuery =
        transformedDF.writeStream
        .format("delta")
        .outputMode(OutputMode.Append())
        .option(DpConstants.DELTA_CHECKPOINT_PATH,fullCheckpointPath)
        .option(DpConstants.COMPRESSION_CODEC,props.getProperty(DpConstants.COMPRESSION_CODEC))
        .option(DpConstants.DELTA_OUTPUT_PATH,fullOutputPath)
        .option(DpConstants.MERGE_SCHEMA,props.getProperty(DpConstants.MERGE_SCHEMA))
        .option(DpConstants.OPTIMIZED_WRITE,props.getProperty(DpConstants.OPTIMIZED_WRITE))
        .option(DpConstants.AUTO_COMPACT,props.getProperty(DpConstants.AUTO_COMPACT))
        .trigger(Trigger.ProcessingTime(props.getProperty(DpConstants.PROCESSING_TIME)))
          .queryName(s"Stream-${dpEvent.getEventKey}")
        .partitionBy(getDPPartitionCols(dpEvent):_*)
        .start()
     writerQuery
    }
    catch {
      case e:Exception => logger.info("Exception while writing data .....")
        throw e
    }
  }

  def tranformDPStream(structuredDF : DataFrame) : DataFrame = {

    logger.info("Starting Transformation on the Data Stream .......")

    try{
      // just for testing purpose , so that we get the data in s3 with the date, when we started consuming from kafka topic

      val transformedDF = structuredDF
        .withColumn("dpYear",col("dpYear").cast(DataTypes.IntegerType))
        .withColumn("dpMonth",col("dpMonth").cast(DataTypes.IntegerType))
        .withColumn("dpDay",col("dpDay").cast(DataTypes.IntegerType))

      logger.info("Transformation completed successfully ........ ")

      transformedDF

    }catch {
      case e:Exception => logger.info("Exception while transforming data stream .....")
        throw e
    }

  }

  def getDPPartitionCols(dpEvent : Event): Seq[String] = {

    var partitionCols = Seq[String]()
    val adminCols = dpEvent.getPartitionCols
    if(adminCols.isEmpty || adminCols == null) return partitionCols
    try{
      partitionCols = adminCols.trim.stripPrefix("[").stripSuffix("]").split(",").map(_.trim).filter(_.nonEmpty).toSeq
      partitionCols
    }catch {
      case e : Exception => logger.info("Exception while transforming partition columns .... ")
        throw e
    }

  }

  def getDPTopic(dpEvent : Event) : String = {
    var topic = new String
    val adminTopic = dpEvent.getTopic
      try{
        topic = adminTopic.trim.stripPrefix("[").stripSuffix("]")
        topic
      }
      catch {
        case e : Exception => logger.info("Exception while transforming topics ..... ")
          throw e
      }
  }


}
