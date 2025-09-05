package com.snapdeal.dp.Streaming.databricks

import com.snapdeal.dataplatform.admin.client.Impl.DataplatformEventKeyinfoClient
import com.snapdeal.dp.Streaming.Admin.DpAdminService
import com.snapdeal.dp.Streaming.databricks.Executor.DPExecutorService
import com.snapdeal.dp.Streaming.databricks.Schema.EventDetails
import com.snapdeal.dp.Streaming.databricks.Schema.Utils.DatabricksSchemaConverter.Field
import com.snapdeal.dp.Streaming.databricks.local.api.TableUtility
import com.snapdeal.dp.Streaming.databricks.local.api.TableUtility.getDatabricksEventSet
import com.snapdeal.dp.constants.DpConstants
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

/**
 * Created by Bhavya Joshi
 * on 27/12/2024
 */

object DBDriver {

  private var dpAdminClient : DataplatformEventKeyinfoClient = _
  private val logger                                         = LoggerFactory.getLogger(TableUtility.getClass)
  private var dpEventSchemaMap : scala.collection.mutable.Map[String,EventDetails] = _

  private var nonTxnDictMap : Map[(String,String),java.util.List[Field]] = Map.empty
  private var txnDictMap : Map[(String,String),java.util.List[Field]]    = Map.empty
  private var spark : SparkSession                           = _
  private var executorService : ExecutorService              = _

  private val txnSuccessCounter : AtomicInteger              = new AtomicInteger(0)
  private val txnFailureCounter : AtomicInteger              = new AtomicInteger(0)
  private val nonTxnSuccessCounter : AtomicInteger           = new AtomicInteger(0)
  private val nonTxnFailureCounter : AtomicInteger           = new AtomicInteger(0)

  /**
   * Created by Bhavya Joshi
   */

  def main (args : Array[String]) = {

    logger.info("###### Starting Local Application context ##########")

    try{
      spark = SparkSession.builder().appName("Dp_LocalService").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("INFO")
      dpAdminClient = DpAdminService.getDPEventKeyInfoClient
      dpEventSchemaMap =  getDatabricksEventSet(dpAdminClient)
      dpEventSchemaMap.foreach{
        event =>
             if(event._2.isTransactional){
               val ek = event._1
               val partitionCols = event._2.partitionCols
                val columns : java.util.List[Field] = event._2.schema.columns // list[ Field(columnName,dataType) , ...]
               txnDictMap = txnDictMap + ((ek,partitionCols) -> columns)
             }

                  if(!event._2.isTransactional){
                    val ek = event._1
                    val partitionCols = event._2.partitionCols
                    val columns : java.util.List[Field] = event._2.schema.columns // list[ Field(columnName,dataType) , ...]
                    nonTxnDictMap = nonTxnDictMap + ((ek,partitionCols) -> columns)
                }

      }

          logger.info("Number of Transactional Events : " +txnDictMap.size)
          logger.info("Number of Non-Transactional Events : " +nonTxnDictMap.size)

      try{
        executorService = Executors.newFixedThreadPool(DpConstants.MAX_THREADS)

//        val txnExecutor = new DPExecutorService(executorService,txnDictMap,txnSuccessCounter,txnFailureCounter,true)
        val nonTxnExecutor = new DPExecutorService(executorService,nonTxnDictMap,nonTxnSuccessCounter,nonTxnFailureCounter,false)
//        txnExecutor.execute()
        nonTxnExecutor.execute()

      }catch {
        case e : Exception => logger.info("Exception while executing API Request ..... ")
      }
      finally {
        executorService.shutdown()
        while(!executorService.isTerminated){
          executorService.awaitTermination(10,TimeUnit.MINUTES)
          logger.info(s"Success API Hits for Transactional Events : ${txnSuccessCounter} \n Failed API Hits : ${txnFailureCounter}")
          logger.info(s"Success API Hits for Non-Transactional Events : ${nonTxnSuccessCounter} \n Failed API Hits : ${nonTxnFailureCounter}")
        }

      }


      logger.info("######### Stopping application context ############")
    }catch {
      case e : Exception => logger.info("Exception while starting service ..... ")
        throw e
    }
    finally {
      if(spark != null){
        spark.sparkContext.stop()
      }
    }

  }

}
