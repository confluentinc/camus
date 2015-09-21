package com.linkedin.camus.coders;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 *
 * @author kgoodhop
 *
 * @param <R> The type of decoded payload
 */
public class CamusWrapper<R> {
    private R record;
    private long timestamp;
    private MapWritable partitionMap = new MapWritable();

    public static final Text SERVER = new Text("server");
    public static final Text SERVICE = new Text("service");
    public static final Text DEFAULT_SERVER = new Text("unknown_server");
    public static final Text DEFAULT_SERVICE = new Text("unknown_service");

    public CamusWrapper() {
        super();
    }

    public void set(R record) {
        this.set(record, System.currentTimeMillis());
    }

    public void set(R record, long timestamp) {
        this.set(record, timestamp, DEFAULT_SERVER, DEFAULT_SERVICE);
    }

    public void set(R record, long timestamp, Text server, Text service) {
        this.record = record;
        this.timestamp = timestamp;
        this.partitionMap.clear();
        partitionMap.put(this.SERVER, server);
        partitionMap.put(this.SERVICE, service);
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Add a value for partitions
     */
    public void put(Writable key, Writable value) {
        partitionMap.put(key, value);
    }

    /**
     * Get a value for partitions
     * @return the value for the given key
     */
    public Writable get(Writable key) {
        return partitionMap.get(key);
    }

    /**
     * Get all the partition key/partitionMap
     */
    public MapWritable getPartitionMap() {
        return partitionMap;
    }

}
