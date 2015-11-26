package com.kenshoo.bigdata.cassandra_to_hive;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
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
public class Reduce extends Reducer<BytesWritable,BytesWritable,Text,Text> {

    private String keyspaceName,columnFamilyName;
    private String cassandraAddress,cassandraUsername,cassandraPassword;
    private Schema avroSchema = new Schema.Parser().parse(Main.CASSANDRA_RECORD_AVRO_SCHEMA);
    private int recordsPerBulk, recordsInBulk = 0;
    private Keyspace keyspace;
    private ColumnFamily<byte[], byte[]> columnFamily;
    MutationBatch mutatorBatch = null;

    public void setup(Context context)
            throws java.io.IOException, InterruptedException {

        //Init from configuration
        {
            Configuration conf = context.getConfiguration();

            cassandraAddress = conf.get("cassandraAddress");
            System.out.println("cassandraAddress: " + cassandraAddress);

            cassandraUsername = conf.get("cassandraUsername");
            System.out.println("cassandraUsername: " + cassandraUsername);

            cassandraPassword = conf.get("cassandraPassword");
            System.out.println("cassandraPassword Length: " + Integer.toString(cassandraPassword.length()));

            keyspaceName = conf.get("keyspaceName");
            System.out.println("keyspaceName: " + keyspaceName);

            columnFamilyName = conf.get("columnFamilyName");
            System.out.println("columnFamilyName: " + columnFamilyName);

            recordsPerBulk = conf.getInt("recordsPerBulk",100);
            System.out.println("recordsPerBulk: " + Integer.toString(recordsPerBulk));
        }

        //Astyanax Init
        {
            AstyanaxContext<Keyspace> astyanaxContext = new AstyanaxContext.Builder()
                    .forCluster(keyspaceName)
                    .forKeyspace(keyspaceName)
                    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                    )
                    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                                    .setPort(9160)
                                    .setMaxConnsPerHost(1)
                                    .setSeeds(cassandraAddress)
                                    .setAuthenticationCredentials(new SimpleAuthenticationCredentials(cassandraUsername, cassandraPassword))
                    )
                    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                    .buildKeyspace(ThriftFamilyFactory.getInstance());

            astyanaxContext.start();
            keyspace = astyanaxContext.getClient();
            columnFamily = new ColumnFamily<byte[], byte[]>(
                    columnFamilyName,// Column Family Name
                    com.netflix.astyanax.serializers.BytesArraySerializer.get(), // Key Serializer
                    com.netflix.astyanax.serializers.BytesArraySerializer.get());

            mutratorBatchCreate();
        }

        context.getCounter("Task", "customSetup").increment(1);
    }

    protected void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        GenericRecord record;

        for(BytesWritable value : values) {
            record = Main.avroDeserializeFromByte(avroSchema,value.getBytes());

            byte[] cassandraKey = ((ByteBuffer)(record.get("key"))).array();
            byte[] cassandraColumnName = ((ByteBuffer)(record.get("columnName"))).array();
            byte[] cassandraValue = ((ByteBuffer)(record.get("value"))).array();
            long cassandraTimestmap = (long) record.get("timestamp");
            int cassandraTtl = (int) record.get("ttl");

            //ASTYANAX
            {
                if(cassandraTimestmap > 0)
                    mutatorBatch.setTimestamp(cassandraTimestmap);

                if(cassandraTtl > 0)
                    mutatorBatch.withRow(columnFamily, cassandraKey).putColumn(cassandraColumnName, cassandraValue,cassandraTtl);
                else
                    mutatorBatch.withRow(columnFamily, cassandraKey).putColumn(cassandraColumnName, cassandraValue);

                context.getCounter("cassandra","addRow").increment(1);
                recordsInBulk++;

                if(recordsInBulk >= recordsPerBulk) {
                    executeWrite(context);
                    mutratorBatchCreate();
                }
            }
        }
    }

    private void executeWrite(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
        mutatorBatch.setTimestamp(3000);

        if(recordsInBulk > 0) {
            boolean wasSuccess = false;
            int retryCount = 3;
            String lastErrorMessage = "";

            while(wasSuccess == false && retryCount >=0) {
                try {
                    OperationResult<Void> result = mutatorBatch.execute();
                    context.getCounter("cassandra","write.Success").increment(1);
                    mutratorBatchCreate();
                    wasSuccess = true;
                } catch (Exception e) {
                    retryCount--;
                    System.out.println("ERROR -> mutator.Batch.execute() -> " + e.getMessage());
                    context.getCounter("cassandra","write.Fail." + retryCount).increment(1);
                    lastErrorMessage = e.getMessage();

                    System.out.println("Cassandra write -> " + e);
                }
            }

            if(wasSuccess == false) {
                context.getCounter("cassandra","write.Fail.Absolute").increment(1);
                throw new IOException("Failed writing to Cassandra -> " + lastErrorMessage);
            }
        }
    }

    private void mutratorBatchCreate()  {
        mutatorBatch = keyspace.prepareMutationBatch();
        recordsInBulk = 0;
    }

    @Override
    public void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {
        //take care of any left over records
        executeWrite(context);

        context.getCounter("Task", "cleanup").increment(1);
    }

}
