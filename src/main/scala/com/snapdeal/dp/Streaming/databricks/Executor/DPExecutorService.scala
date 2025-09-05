package com.snapdeal.dp.Streaming.databricks.Executor

import com.snapdeal.dp.Streaming.databricks.Schema.Utils.DatabricksSchemaConverter.Field
import com.snapdeal.dp.Streaming.databricks.local.api.DatabricksApiService
import org.slf4j.LoggerFactory

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger


/**
 * Created by Bhavya Joshi
 */

class DPExecutorService(executorService : ExecutorService
                        , dictMap : Map[(String,String),java.util.List[Field]]
                        , successCounter : AtomicInteger
                        , failureCounter : AtomicInteger
                        ,isTransactional : Boolean) extends Serializable{

  private val logger = LoggerFactory.getLogger(classOf[DPExecutorService])

  def execute(): Unit = {
    dictMap.foreach {
      case ((eventName,partitionCols), schemaList) =>
        executorService.submit(new Runnable {
          override def run(): Unit = {
            try {
              val apiService = new DatabricksApiService()
              val response = apiService.createTable(eventName, schemaList,partitionCols,isTransactional)
              if (!response.code.equals(200)) {
                failureCounter.incrementAndGet()
                logger.info(s"Response for $eventName: ${response.body}")
              } else successCounter.incrementAndGet()
            } catch {
              case e: Exception =>
                logger.error(s"Exception occurred while processing $eventName", e)
            }
          }
        })
    }
  }

}
