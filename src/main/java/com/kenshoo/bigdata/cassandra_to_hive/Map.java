package com.kenshoo.bigdata.cassandra_to_hive;



import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hive.com.esotericsoftware.kryo.serializers.DefaultArraySerializers;
import org.apache.hive.hcatalog.common.*;
import org.apache.hive.hcatalog.mapreduce.*;
import org.apache.hive.hcatalog.data.*;
import org.apache.hive.hcatalog.data.schema.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by noamh on 21/07/15.
 */
//public class Map extends Mapper<WritableComparable, HCatRecord, Text, MapWritable>{
//AVRO
//public class Map extends Mapper<WritableComparable, HCatRecord, Text, AvroValue<GenericRecord>> {
public class Map extends Mapper<WritableComparable, HCatRecord, BytesWritable, BytesWritable> {
    //private HCatSchema inputSchema = null,outputSchema = null;
    private HCatSchema inputSchema = null;
    private DefaultHCatRecord outputRecord;

    private String keyFieldName,columnNameFieldName,valueFieldName;
    private String timestampFieldName,ttlFieldName;

    //AVRO
    Schema avroSchema = new Schema.Parser().parse(Main.CASSANDRA_RECORD_AVRO_SCHEMA);

    @Override
    public void setup(Context context)
            throws java.io.IOException, InterruptedException{

        //Init from configuration
        {
            Configuration conf = context.getConfiguration();

            keyFieldName = conf.get("keyFieldName");
            System.out.println("keyFieldName: " + keyFieldName);

            columnNameFieldName = conf.get("columnNameFieldName");
            System.out.println("columnNameFieldName: " + columnNameFieldName);

            valueFieldName = conf.get("valueFieldName");
            System.out.println("valueFieldName: " + valueFieldName);

            timestampFieldName = conf.get("timestampFieldName");
            System.out.println("timestampFieldName: " + timestampFieldName);

            ttlFieldName = conf.get("ttlFieldName");
            System.out.println("ttlFieldName: " + ttlFieldName);
        }

        //Schema Init
        {
            inputSchema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());
        }

        context.getCounter("Task", "setup").increment(1);
    }

    @Override
    public void map(WritableComparable key, HCatRecord value,
                    org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, BytesWritable, BytesWritable>.Context context)
            throws java.io.IOException, InterruptedException {

        context.getCounter("Map.Custom","GotRecord").increment(1);

        byte[] cassandraKey = value.getByteArray(keyFieldName,inputSchema);
        byte[] cassandraColumnName = value.getByteArray(columnNameFieldName,inputSchema);
        byte[] cassandraValue = value.getByteArray(valueFieldName,inputSchema);
        long cassandraTimestmap = 0;
        int cassandraTtl = 0;

        //timestamp & ttl are optional
        if(timestampFieldName != null && timestampFieldName.isEmpty() == false) {
            cassandraTimestmap = value.getLong(timestampFieldName,inputSchema);
        }
        if(ttlFieldName != null && ttlFieldName.isEmpty() == false) {
            cassandraTtl = value.getInteger(ttlFieldName,inputSchema);
        }

        //AVRO
        GenericRecord cassandraRecord = new GenericData.Record(avroSchema);
        cassandraRecord.put("key", ByteBuffer.wrap(cassandraKey));
        cassandraRecord.put("columnName",ByteBuffer.wrap(cassandraColumnName));
        cassandraRecord.put("value",ByteBuffer.wrap(cassandraValue));
        cassandraRecord.put("ttl",cassandraTtl);
        cassandraRecord.put("timestamp", cassandraTimestmap);
        context.write(new BytesWritable(cassandraKey),new BytesWritable(Main.avroSerializeToByte(cassandraRecord)));

    }



}
