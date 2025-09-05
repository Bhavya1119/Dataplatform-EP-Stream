package com.snapdeal.dp.Streaming.databricks.local.api

import com.snapdeal.dp.Streaming.databricks.Schema.Utils.DatabricksSchemaConverter.Field
import com.snapdeal.dp.constants.DpConstants
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

/**
 * Created by Bhavya Joshi
 */

class DatabricksApiService {

  private val logger = LoggerFactory.getLogger(classOf[DatabricksApiService])

  private val CATALOG = "dataplatform_api"
  private val SCHEMA = util.Arrays.asList("Txn","NonTxn")
  private val WAREHOUSE_ID = "YOUR_WAREHOUSE_ID"
  private val API_ENDPOINT = "YOUR_ENDPOINT"

  def createTable(eventName : String, eventSchemaMap : java.util.List[Field],partitionCols : String , isTransactional : Boolean) : HttpResponse[String] = {

    logger.info(s"#################### Executing Create Table Command for - ${eventName}  ###############################")
    try{
      val createCommand = generateCreateCommand(eventName,eventSchemaMap,partitionCols,isTransactional)
      val jsonBody = getJsonBody(CATALOG,SCHEMA,WAREHOUSE_ID,isTransactional, createCommand)
      val response = executeCommand(API_ENDPOINT,jsonBody)
      response
    }
    catch {
      case e : Exception => logger.info(s"Exception while creating Table $eventName ...")
        throw e
    }

  }

  private[databricks] def generateCreateCommand(eventName: String, eventSchemaList: java.util.List[Field], partitionCols: String, isTransactional: Boolean) = {
    //check if event is transactional or not and set the base path
    val basePath = if(isTransactional) DpConstants.TXN_PATH else DpConstants.NON_TXN_PATH

    val columns = new StringBuilder()
    val iter = eventSchemaList.asScala.iterator
    while(iter.hasNext) {
      val column = iter.next()
      columns.append(column.getColumnName).append(" ").append(column.getDataType)
      if(iter.hasNext) columns.append(", ")
    }

    val createCommand = new StringBuilder()
      .append("CREATE TABLE IF NOT EXISTS ")
      .append(eventName)
      .append(" (")
      .append(columns)
      .append(") USING DELTA")

    // if partition columns exists for that event => partition the table by those columns
    if(partitionCols != null ) {
      createCommand.append(" PARTITIONED BY (").append(partitionCols).append(")")
    }

    createCommand.append(" LOCATION '").append(basePath).append(eventName).append("'")

    val result = createCommand.toString()
    logger.debug(s"Create Table Command Generated for $eventName : \n $result")
    result
  }




  private[databricks] def getJsonBody(catalog: String, schema: java.util.List[String], warehouse_id: String, isTransactional: Boolean, createCommand: String) = try {
    val jsonBody = if (isTransactional)
      s"""
          {
          "warehouse_id": "$warehouse_id",
          "catalog": "$catalog",
          "schema": "${schema.get(0)}",
          "statement": "$createCommand"
         }
         """ else
      s"""
          {
          "warehouse_id": "$warehouse_id",
          "catalog": "$catalog",
          "schema": "${schema.get(1)}",
          "statement": "$createCommand"
         }
         """

    logger.debug(s"Generated JSON body: $jsonBody")
    jsonBody

  } catch {
    case e: Exception =>
      logger.error(s"Exception while fetching jsonBody: ${e.getMessage}")
      throw e
  }



  private[databricks] def executeCommand(apiEndpoint : String, jsonBody : String) = try{
    val response = Http(apiEndpoint)
      .timeout(connTimeoutMs = 600000, readTimeoutMs = 600000)
      .header("content-type", "application/json")
      .header("Authorization" , "YOUR_TOKEN")
      .postData(jsonBody)
      .asString

    response
  }
  catch {
    case e : Exception => logger.info("Exception while executing API request ......")
      throw e
  }
}
