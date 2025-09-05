package com.snapdeal.dp.DPEvent;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by Bhavya Joshi
 */

/**
 * DP Event Entity
 */
public class Event implements Serializable {
    private String eventKey;
    private String topic;
    private String bucketID;
    private String partitionCols ;

    public Event(){}

    public Event(String eventKey, String topic, String bucketID, String partitionCols) {
        this.eventKey = eventKey;
        this.topic = topic;
        this.bucketID = bucketID;
        this.partitionCols = partitionCols;
    }
    public Event(String eventKey,String topic , String bucketID){
        this.eventKey = eventKey;
        this.topic = topic;
        this.bucketID = bucketID;
    }

    public String getEventKey() {
        return eventKey;
    }

    public void setEventKey(String eventKey) {
        this.eventKey = eventKey;
    }

    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBucketID() {
        return bucketID;
    }

    public void setBucketID(String bucketID) {
        this.bucketID = bucketID;
    }

    public String getPartitionCols() {
        return partitionCols;
    }


    public void setPartitionCols(String partitionCols) {
        this.partitionCols = partitionCols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(eventKey, event.eventKey) && Objects.equals(topic, event.topic) && Objects.equals(bucketID, event.bucketID) && Objects.equals(partitionCols, event.partitionCols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventKey, topic, bucketID, partitionCols);
    }

    @Override
    public String toString() {
        return "Event{" +
                "eventKey='" + eventKey + '\'' +
                ", topic='" + topic + '\'' +
                ", bucketID='" + bucketID + '\'' +
                ", partitionCols=" + partitionCols +
                '}';
    }
}
