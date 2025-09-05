package com.snapdeal.dp.Streaming.Admin


import com.snapdeal.dataplatform.admin.base.model.EventKeyInfoSRO
import com.snapdeal.dataplatform.admin.base.response.AllEventKeyInfosResponse
import com.snapdeal.dataplatform.admin.base.utils.FileUtils
import com.snapdeal.dataplatform.admin.client.Impl.{DataplatformAdminClientImpl, DataplatformEventKeyinfoClient}
import com.snapdeal.dp.DPEvent.Event
import com.snapdeal.dp.constants.DpConstants
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

import java.util
import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

/**
 * Created by Bhavya Joshi
 * on 2/12/24
 */

object DpAdminService {

  private val logger = LoggerFactory.getLogger(DpAdminService.getClass)

  private var properties : Properties = null
  private var dpAdminClient : DataplatformAdminClientImpl = _
  private var dpEventKeyInfoClient : DataplatformEventKeyinfoClient = _
  private var allEventKeysInfoMap : Map[String,EventKeyInfoSRO] = _
  private var allTxnEventKeysSet : java.util.HashSet[String] = _
  private var allNonTxnEventKeysSet : java.util.HashSet[String] = _


  def runWithTimeout[T](timeoutMs: Long)(f: => T): Unit = {
    Await.result(Future(f), Duration(timeoutMs, "millis")).asInstanceOf[Unit]
  }

  def getDPAdminClient : DataplatformAdminClientImpl = {
    properties = FileUtils.loadPropertiesFromClassPath("app.properties")
    logger.info("Getting DP Admin client ..... ")
    if(dpAdminClient == null) {
      logger.info("Admin client is null. Creating a new DP Admin client .....")

      var appContext : ApplicationContext = null
      try{
        runWithTimeout(10000){
          appContext = new ClassPathXmlApplicationContext("applicationContextDpAdminClient.xml")
        }
      }catch {
        case e: TimeoutException =>
          throw new RuntimeException("DP Admin context creation took more than 10 seconds ....")
      }
      logger.info("Getting DP Admin client bean ...... ")

      dpAdminClient = appContext.getBean("dataplatformAdminClientService").asInstanceOf[DataplatformAdminClientImpl]


      logger.info("DP Admin client created .....")
      dpAdminClient.setWebServiceBaseURL(properties.getProperty(DpConstants.ADMIN_WEBSERVICE_BASE_URL))
    }
    logger.info("DP Admin client call completed ....")
    dpAdminClient
  }


  def getDPEventKeyInfoClient : DataplatformEventKeyinfoClient = {
    properties = FileUtils.loadPropertiesFromClassPath("app.properties")
    logger.info("Getting DP EK Admin client ..... ")
    if(dpEventKeyInfoClient == null) {
      logger.info("Admin client is null. Creating a new DP EK Admin client .....")

      var appContext : ApplicationContext = null
      try{
        runWithTimeout(10000){
          appContext = new ClassPathXmlApplicationContext("applicationContextDpAdminClient.xml")
        }
      }catch {
        case e: TimeoutException =>
          throw new RuntimeException("DP Admin context creation took more than 10 seconds ....")
      }
      logger.info("Getting DP EK Admin client bean ...... ")

      dpEventKeyInfoClient = appContext.getBean("dataplatformEventKeyAdminClientService").asInstanceOf[DataplatformEventKeyinfoClient]

      logger.info("DP Admin client created .....")
      dpEventKeyInfoClient.setWebServiceBaseURL(properties.getProperty(DpConstants.ADMIN_WEBSERVICE_BASE_URL))
    }
    logger.info("DP Admin client call completed ....")
    dpEventKeyInfoClient
  }

  def getAllEventKeyInfoMap : Map[String,EventKeyInfoSRO] = {
    if(allEventKeysInfoMap == null){
      logger.info("All EventKey map is empty, Populating Map ........")
      var allEventKeysInfos: Option[AllEventKeyInfosResponse] = None
      try{
        val eventKeyInfoList = getDPEventKeyInfoClient.getAllEventKeyInfos
        allEventKeysInfos = Option(eventKeyInfoList)
        if(allEventKeysInfos.isEmpty) {
          logger.info("Not able to fetch event Key info ......")
        }
      }catch {
        case e:Exception => logger.info("Exception while getting EventKeyInfo .... ")
          e.printStackTrace()
      }

      if(allEventKeysInfos.isDefined){
        val allEventKeyInfoList = allEventKeysInfos.get.getAllEventKeyInfoSROS.asScala.toList
        allEventKeysInfoMap = allEventKeyInfoList.map(x => x.getEventKey -> x).toMap
      }
      else{
        throw new Exception("No response from dp admin eventKeyinfo service")
      }

    }
    allEventKeysInfoMap
  }

  def getTxnEvents(dpAdminInfoClient: DataplatformEventKeyinfoClient): java.util.HashSet[String] = {
    if(allTxnEventKeysSet == null) {
      allTxnEventKeysSet = new java.util.HashSet[String]()
      try {
        var allEventKeyInfos = Option(dpAdminInfoClient.getAllEventKeyInfos)
        if (allEventKeyInfos.isEmpty) {
          logger.info("Not able to fetch eventKey Info .... ")
        }
        if (allEventKeyInfos.isDefined) {
          val allEventKeyInfoList = allEventKeyInfos.get.getAllEventKeyInfoSROS.asScala.toList
          allEventKeyInfoList.foreach(eventKeySRO => {
            if (eventKeySRO.getIsTransactional != null && eventKeySRO.getIsTransactional) {
              allTxnEventKeysSet.add(eventKeySRO.getEventKey)
            }
          })
        }
      } catch {
        case e: Exception =>
          logger.error("Exception while getting txn events ...", e)
      }
    }
    allTxnEventKeysSet
  }

  def getNonTxnEvents(dpAdminInfoClient: DataplatformEventKeyinfoClient): java.util.HashSet[String] = {
    if(allNonTxnEventKeysSet == null) {
      allNonTxnEventKeysSet = new java.util.HashSet[String]()
      try {
        var allEventKeyInfos = Option(dpAdminInfoClient.getAllEventKeyInfos)
        if (allEventKeyInfos.isEmpty) {
          logger.info("Not able to fetch eventKey Info .... ")
        }
        if (allEventKeyInfos.isDefined) {
          val allEventKeyInfoList = allEventKeyInfos.get.getAllEventKeyInfoSROS.asScala.toList
          allEventKeyInfoList.foreach(eventKeySRO => {
            if (eventKeySRO.getIsTransactional != null && eventKeySRO.getIsTransactional.toString.equals("false")) {
              allNonTxnEventKeysSet.add(eventKeySRO.getEventKey)
            }
          })
        }
      } catch {
        case e: Exception =>
          logger.error("Exception while getting txn events ...", e)
      }
    }
    allNonTxnEventKeysSet
  }

}

