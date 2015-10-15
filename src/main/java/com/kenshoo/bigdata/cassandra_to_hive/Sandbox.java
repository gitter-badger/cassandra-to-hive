package com.kenshoo.bigdata.cassandra_to_hive;

/*
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Using;
import com.datastax.driver.core.schemabuilder.UDTType;
*/

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by noamh on 19/07/15.
 */
public class Sandbox {
    public static String cassandraSchema = "{\"namespace\": \"kenshoo.com\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"CassandraRecord\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"key\", \"type\": \"bytes\"},\n" +
            "     {\"name\": \"columnName\", \"type\": \"bytes\"},\n" +
            "     {\"name\": \"value\", \"type\": \"bytes\"},\n" +
            "     {\"name\": \"ttl\", \"type\": \"long\"},\n" +
            "     {\"name\": \"timestamp\", \"type\": \"long\"}\n" +
            " ]\n" +
            "}";

    public static void main(String[] args) throws IOException {
        /*
        MapWritable mapWritable = new MapWritable();

        mapWritable.put(new Text("test"), new LongWritable(5));

        LongWritable value =  (LongWritable) mapWritable.get(new Text("test"));

        System.out.println(value.toString());


        byte[] myArray = new byte[] {1,2,3,4};
        BytesWritable bytesWritable = new BytesWritable(myArray);
        mapWritable.put(new Text("test2"),bytesWritable);

        myArray = ((BytesWritable) mapWritable.get(new Text("test2"))).getBytes();

        System.out.println(myArray.length);
        */



        Schema schema = new Schema.Parser().parse(cassandraSchema);
        GenericRecord cassandraRecord = new GenericData.Record(schema);
        byte[] array = new byte[] {1,1,2,3,4};
        cassandraRecord.put("cassandraKey",ByteBuffer.wrap(array));
        cassandraRecord.put("timestamp",1234l);



        byte[] bytes = serializeToByte(cassandraRecord);

        cassandraRecord = deserializeFromByte(schema,bytes);

        System.out.println(cassandraRecord.get("timestamp"));
        System.out.println(((ByteBuffer) cassandraRecord.get("cassandraKey")).array().length);

        /*
        String keyspace = "dejavu";
        String columnFamily = "events";
        System.out.println("Hello World");


        Cluster cluster = Cluster.builder().addContactPoint("10.73.210.11").withCredentials("cassandra","cassandra").build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());

        Session session = cluster.connect();



        /*
        //events
        Statement statement = QueryBuilder.insertInto(keyspace, columnFamily)
                .value("key", "hello")
                .value("column1", UUID.fromString("ac78f1d4-ed00-47a3-bd83-7070fd1a537e"))
                .value("column2",UUID.fromString("9a6fe800-162e-11e5-cd87-7c8d27d807bc"))
                .value("value","hello world")
                .using(QueryBuilder.ttl(30));
        */

        //index_lookup
        long number = 100;
        /*
        //"PROFILE_ID",UUID.fromString(groupToken),Integer.toString(profileId),keyTimestamp
        Statement statement = QueryBuilder.insertInto(keyspace, "index_lookup")
                .value("key", "PROFILE_ID")
                .value("key2", UUID.fromString("9a6fe800-162e-11e5-cd87-7c8d27d807bc"))
                .value("key3", "Test")
                .value("key4", 100)
                .value("column1", UUID.fromString("9a6fe800-162e-11e5-cd87-7c8d27d807bc"))
                .value("value","hello world");
        */
        //"ORDER_ID",UUID.fromString(groupToken),orderId
        /*
        Statement statement = QueryBuilder.insertInto(keyspace, "index_lookup")
                .value("key", "ORDER_ID")
                .value("key2", UUID.fromString("9a6fe800-162e-11e5-cd87-7c8d27d807bc"))
                .value("key3", "100")
                .value("key4",0)
                .value("column1", UUID.fromString("9a6fe800-162e-11e5-cd87-7c8d27d807bc"))
                .value("value","hello world");




        session.execute(statement);
        System.out.println("Done");

        //session.closeAsync();
        */
    }

    public static byte[] serializeToByte(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

        writer.write(record, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    public static GenericRecord deserializeFromByte(Schema schema,byte[] bytes) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = reader.read(null, decoder);
        return record;
    }
}
