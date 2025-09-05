package com.snapdeal.dp.Streaming.databricks.local.Entity;

import java.util.Objects;

/**
 * Created by Bhavya Joshi
 */

public class DBEvent {
    private String eventKey;
    private String schema;
    private Boolean isTransactional;
    private String partitionCols;

    public DBEvent(){}

    public DBEvent(String eventKey, String schema) {
        this.eventKey = eventKey;
        this.schema = schema;
    }
    public DBEvent(String eventKey, String schema,String partitionCols, Boolean isTransactional){
        this.eventKey = eventKey;
        this.schema = schema;
        this.isTransactional = isTransactional;
        this.partitionCols = partitionCols;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getEventKey() {
        return eventKey;
    }

    public void setEventKey(String eventKey) {
        this.eventKey = eventKey;
    }

    public Boolean getIsTransactional() {
        return isTransactional;
    }

    public void setIsTransactional(Boolean transactional) {
        isTransactional = transactional;
    }

    public String getPartitionCols(){return partitionCols ;}

    @Override
    public String toString() {
        return "DBEvent{" +
                "eventKey='" + eventKey + '\'' +
                ", schema='" + schema + '\'' +
                ", isTransactional=" + isTransactional +
                ", partitionCols='" + partitionCols + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBEvent dbEvent = (DBEvent) o;
        return Objects.equals(eventKey, dbEvent.eventKey) && Objects.equals(schema, dbEvent.schema) && Objects.equals(isTransactional, dbEvent.isTransactional) && Objects.equals(partitionCols, dbEvent.partitionCols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventKey, schema, isTransactional, partitionCols);
    }
}
