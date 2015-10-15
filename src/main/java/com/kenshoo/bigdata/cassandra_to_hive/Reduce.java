package com.kenshoo.bigdata.cassandra_to_hive;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;
import sun.net.www.content.text.Generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created by noamh on 30/08/15.
 */
//public class Reduce extends Reducer<Text,MapWritable,Text,Text> {
//AVRO
//public class Reduce extends Reducer<Text,AvroValue<GenericRecord>,Text,Text> {
public class Reduce extends Reducer<BytesWritable,BytesWritable,Text,Text> {
    //AVRO
    private Schema avroSchema = new Schema.Parser().parse(Sandbox.cassandraSchema);

    public void setup(Context context)
            throws java.io.IOException, InterruptedException {

        context.getCounter("Task", "customSetup").increment(1);
    }

//    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
//        for(MapWritable value : values) {
//
//            byte[] cassandraKey = byteArrayGet(value,"cassandraKey");
//            System.out.println("cassandraKey: " + cassandraKey.length);
//
//            byte[] cassandraColumnName =  byteArrayGet(value,"cassandraColumnName");
//
//
//
//            long cassandraTimestamp = longGet(value,"cassandraTimestamp");
//        }
//    }

    //protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
    //AVRO
    //protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {
    protected void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        GenericRecord record;

        for(BytesWritable value : values) {

            record = Sandbox.deserializeFromByte(avroSchema,value.getBytes());
            byte[] cassandraKey = ((ByteBuffer)(record.get("cassandraKey"))).array();
            byte[] cassandraColumnName = ((ByteBuffer)(record.get("columnName"))).array();
            byte[] cassandraValue = ((ByteBuffer)(record.get("value"))).array();
            long cassandraTimestmap = (long) record.get("timestamp");
            long ttl = (long) record.get("ttl");
        }
    }

}
