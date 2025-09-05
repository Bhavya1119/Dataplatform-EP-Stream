package com.snapdeal.dp.Streaming.databricks.local.api

import com.snapdeal.dataplatform.admin.client.Impl.DataplatformEventKeyinfoClient
import com.snapdeal.dp.Streaming.databricks.Schema.Utils.DatabricksSchemaConverter
import com.snapdeal.dp.Streaming.databricks.Schema.{EventDetails, SQLSchema}
import com.snapdeal.dp.Streaming.databricks.local.Entity.DBEvent

import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
 * Created by Bhavya Joshi
 * on 29/12/2024
 */

object TableUtility {

  private val logger = LoggerFactory.getLogger(TableUtility.getClass)


  def getDatabricksEventSet( dpAdminClient : DataplatformEventKeyinfoClient) : scala.collection.mutable.Map[String,EventDetails] ={

    val eventSchemaMap : scala.collection.mutable.Map[String,EventDetails] = scala.collection.mutable.Map()

    logger.debug("Fetching EventKey details from dpAdmin ...... ")

    val allEventKeyInfos  = Option(dpAdminClient.getAllEventKeyInfos)
    if(allEventKeyInfos.isEmpty) logger.info("Not able to fetch EventKeyInfo ....")
    if(allEventKeyInfos.isDefined){
      try{
        val allEventKeyInfoList = allEventKeyInfos.get.getAllEventKeyInfoSROS.asScala.toList
        allEventKeyInfoList.foreach(SRO =>
            if (SRO.getIsActive) {
              val schemaConverter = new DatabricksSchemaConverter()
              val eventName = SRO.getEventKey
              val schema = SRO.getCompleteSchema
              val isTransactional = SRO.getIsTransactional

              val partitionCols = Option(SRO.getPartitionColumns) match{
                case Some(cols) if cols.nonEmpty => cols.stripPrefix("[").stripSuffix("]")
                case _                           => null
              }

              if (eventName != null && schema != null && isTransactional != null) {
                //partition columns can be null : In that case we won't create a partitioned table
                val databricksEvent = new DBEvent(eventName, schema, partitionCols, isTransactional)
                try
                {
                  val sqlSchema : SQLSchema = schemaConverter.convertToSQL(databricksEvent)
                  eventSchemaMap.put(databricksEvent.getEventKey, EventDetails(sqlSchema, isTransactional,databricksEvent.getPartitionCols))
                } catch {
                  case e: Exception => logger.info(s"Exception while getting eventSchema for event ${databricksEvent.getEventKey}")
                }
              }
          })

      }
      catch {
        case e : Exception => logger.info("Exception while parsing json response from dpAdmin .... ")
          throw e
      }
    }
    eventSchemaMap
  }




}
