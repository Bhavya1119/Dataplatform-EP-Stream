package com.snapdeal.dp.Streaming.Bucket

import com.snapdeal.dataplatform.admin.client.Impl.DataplatformEventKeyinfoClient
import com.snapdeal.dp.DPEvent.Event
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter


/**
 * Created by Bhavya Joshi
 */

class BucketingService extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[BucketingService])

  private var dpBuckets = new util.ArrayList[(String,util.HashSet[Event])]()
  private var nonP0Event = new util.HashSet[Event]()
  private var p0Event = new util.HashSet[Event]()


  def performBucketing(adminClient : DataplatformEventKeyinfoClient) : java.util.List[(String,java.util.HashSet[Event])] = {
    logger.info("Performing bucketing for DP Events ...... ")
    var bucketSet = new util.HashSet[Event]()
    bucketSet = getDPBuckets(adminClient)
    bucketSet.forEach(event => bucketize(event))
    dpBuckets.add(("p0Event",p0Event))
    dpBuckets.add(("nonP0Event",nonP0Event))
    //    dpBuckets.addAll(util.Arrays.asList(p0Event,nonP0Event))
    dpBuckets
  }

  def bucketize(dpEvent : Event) : Unit = {
    val bucketID = dpEvent.getBucketID
    bucketID match {
      case "p0Event"       =>   logger.debug(s"Matching bucket found ${bucketID}")
        p0Event.add(dpEvent)
      case "nonP0Event"    =>    logger.debug(s"Matching bucket found ${bucketID}")
        nonP0Event.add(dpEvent)
      case  _              =>    logger.debug("no matching Buckets Found .... ")
    }

  }

  def getDPBuckets(dpAdminInfoClient : DataplatformEventKeyinfoClient) : util.HashSet[Event] ={
    val eventSet = new util.HashSet[Event]()
    val allEventKeyInfos = Option(dpAdminInfoClient.getAllEventKeyInfos)
    if(allEventKeyInfos.isEmpty) logger.info("Not able to fetch EventKeyInfo ....")
    if(allEventKeyInfos.isDefined) try {
      val allEventKeyInfoList = allEventKeyInfos.get.getAllEventKeyInfoSROS.asScala.toList
      allEventKeyInfoList.foreach {
        SRO =>
          if (SRO.getIsActive) {
            val eventName = Option(SRO.getEventKey)
            val topic = Option(SRO.getTopicList)
            val bucketID = Option(SRO.getBucketID)
            val partitionCols = Option(SRO.getPartitionColumns)

            (eventName, topic, bucketID, partitionCols) match {
              case (Some(ek), Some(t), Some(bid),Some(parts)) if ek.nonEmpty && t.nonEmpty && bid.nonEmpty && parts.nonEmpty =>
                val event = new Event(ek, t, bid,parts)
                eventSet.add(event)
              case _ =>
                logger.debug(s"Skipping event creation due to null or empty values: eventKey=$eventName, topic=$topic, bucketID=$bucketID , partitionCols=$partitionCols")
            }
          }
      }
      eventSet
    }
    catch {
      case e: Exception => logger.info("Exception while Fetching DP Events ..... ")
        throw e
    }
    else eventSet
  }

}
