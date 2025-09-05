package com.snapdeal.dp.constants;



/**
 * Created by Bhavya Joshi
 */
public class DpConstants {
    private DpConstants(){}
    public final static String KAFKA_TOPIC = "subscribe";
    public final static String KAFKA_BROKERS = "kafka.bootstrap.servers";
    public final static String KAFKA_GROUP = "group.id";
    public final static String SCHEMA_REGISTRY = "kafka.schema.registry";
    public final static String AVRO_MODE = "mode";
    public final static String STREAM_FORMAT = "spark.read.stream.format";
    public final static String STARTING_OFFSETS = "startingOffsets";
    public final static String FAIL_ON_DATA_LOSS = "failOnDataLoss";
    public final static String PROCESSING_TIME = "process.for";
    public final static Integer MAX_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    public final static String DELTA_OUTPUT_PATH = "path";
    public final static String COMPRESSION_CODEC = "compression";
    public final static String DELTA_CHECKPOINT_PATH = "checkpointLocation";
    public final static String MERGE_SCHEMA = "mergeSchema";
    public final static String DELTA_SQL_EXTENSION  = "spark.sql.extensions";
    public final static String SPARK_CATALOG = "spark.sql.catalog.spark_catalog";
    public final static String OPTIMIZED_WRITE = "delta.autoOptimize.optimizeWrite";
    public final static String AUTO_COMPACT = "delta.autoOptimize.autoCompact";


    public final static String ACCESS_KEY = "spark.hadoop.fs.s3a.access.key";
    public final static String SECRET_KEY = "spark.hadoop.fs.s3a.secret.key";
    public final static String S3_ENDPOINT = "spark.hadoop.fs.s3a.endpoint";
    public final static String S3_IMPL = "fs.s3a.impl";
    public final static String S3_CREDENTIAL_PROVIDER = "spark.hadoop.fs.s3a.aws.credentials.provider";

    public static final String ADMIN_WEBSERVICE_BASE_URL = "web.service.base.url";

    public static final String TXN_PATH = "s3://databricks-workspace-stack-65deb-bucket/dataplatform-api/Txn/";
    public static final String NON_TXN_PATH = "s3://databricks-workspace-stack-65deb-bucket/dataplatform-api/NonTxn/";

}
