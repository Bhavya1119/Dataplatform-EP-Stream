package com.snapdeal.dp;

import com.snapdeal.dataplatform.admin.client.Impl.DataplatformEventKeyinfoClient;
import com.snapdeal.dp.DPEvent.Event;
import com.snapdeal.dp.Streaming.Admin.DpAdminService;
import com.snapdeal.dp.Streaming.Bucket.BucketingService;
import com.snapdeal.dp.Streaming.StreamUtil;
import com.snapdeal.dp.constants.DpConstants;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Bhavya Joshi
 */
public class Driver {
    private static Logger LOG = LoggerFactory.getLogger(Driver.class);
    private static SparkSession spark = null;
    private static DataplatformEventKeyinfoClient dpAdminClient = null;
    private static List<Tuple2<String,HashSet<Event>>> dpBucket = null;


    public static void main(String[] args) {
        LOG.info("################# Starting Driver context #####################");
        StreamUtil.init();
        BucketingService bucketingService = new BucketingService();
        spark = StreamUtil.getSparkSession();
        dpAdminClient = DpAdminService.getDPEventKeyInfoClient();
        dpBucket = bucketingService.performBucketing(dpAdminClient);
        ExecutorService executorService = Executors.newFixedThreadPool(DpConstants.MAX_THREADS);
        Object streamLock = new Object();

        for(Tuple2<String,HashSet<Event>> bucket : dpBucket) {
            try {
                String bucketName = bucket._1();
                LOG.info(" ####################### Starting Processing for Bucket - " +bucketName +" ###############################");
                HashSet<Event> bucketSet = bucket._2();
                for (Event event : bucketSet) {
                    executorService.submit(() -> {
                        try {
                            synchronized (streamLock) {
                                StreamUtil.streamKafka(event);
                            }
                        } catch (Exception ex) {
                            LOG.info("Exception encountered by Thread - " + Thread.currentThread().getId() + " while streaming event - " + event.getEventKey());
                            throw ex;
                        }
                    });
                }
            }
            catch (Exception ex1){
                LOG.info("Exception while streaming for Bucket - " +bucket);
            }
        }
        try{
                executorService.shutdown();
                if(!executorService.awaitTermination(60, TimeUnit.MINUTES)){
                    executorService.shutdownNow();
                }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }


    }
}
