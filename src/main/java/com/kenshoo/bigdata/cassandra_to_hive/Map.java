package com.kenshoo.bigdata.cassandra_to_hive;



import java.io.IOException;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
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
public class Map extends Mapper<WritableComparable, HCatRecord, Text, DefaultHCatRecord>{


    //private HCatSchema inputSchema = null,outputSchema = null;
    private HCatSchema inputSchema = null;
    private DefaultHCatRecord outputRecord;

    private String cassandraAddress,cassandraUsername,cassandraPassword;
    private String keyspaceName,columnFamilyName;
    private String keyFieldName,columnNameFieldName,valueFieldName;
    private String timestampFieldName,ttlFieldName;
    private int recordsPerBulk, recordsInBulk = 0;
    private Keyspace keyspace;
    private ColumnFamily<byte[], byte[]> columnFamily;
    MutationBatch mutatorBatch = null;

    @Override
    public void setup(Context context)
            throws java.io.IOException, InterruptedException{

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

            recordsPerBulk = conf.getInt("recordsPerBulk",100);
            System.out.println("recordsPerBulk: " + Integer.toString(recordsPerBulk));
        }

        //Schema Init
        {
            inputSchema = HCatBaseInputFormat.getTableSchema(context.getConfiguration());
            //outputSchema = HCatBaseOutputFormat.getTableSchema(context.getConfiguration());
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

        context.getCounter("Task", "setup").increment(1);
    }

    @Override
    public void map(WritableComparable key, HCatRecord value,
                    org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, Text, DefaultHCatRecord>.Context context)
            throws java.io.IOException {

        context.getCounter("Map.Custom","GotRecord").increment(1);

        byte[] cassandraKey = value.getByteArray(keyFieldName,inputSchema);
        byte[] cassandraColumnName = value.getByteArray(columnNameFieldName,inputSchema);
        byte[] cassandraValue = value.getByteArray(valueFieldName,inputSchema);
        long cassandraTimestmap = 0;
        int cassandraTtl = 7200;

        //timestamp & ttl are optional
        if(timestampFieldName != null && timestampFieldName.isEmpty() == false) {
            cassandraTimestmap = value.getLong(timestampFieldName,inputSchema);
        }
        if(ttlFieldName != null && ttlFieldName.isEmpty() == false) {
            cassandraTtl = value.getInteger(ttlFieldName,inputSchema);
        }

        //ASTYANAX
        {
            if(cassandraTimestmap > 0)
                mutatorBatch.setTimestamp(cassandraTimestmap);

            if(cassandraTtl > 0)
                mutatorBatch.withRow(columnFamily, cassandraKey).putColumn(cassandraColumnName, cassandraValue,cassandraTtl);
            else
                mutatorBatch.withRow(columnFamily, cassandraKey).putColumn(cassandraColumnName, cassandraValue);

            //if(cassandraTimestmap > 0)
            //    column.setTimestamp(cassandraTimestmap);
            //column.setTimestamp(5000);

            context.getCounter("cassandra","addRow").increment(1);
            recordsInBulk++;

            if(recordsInBulk >= recordsPerBulk) {
                executeWrite(context);
                mutratorBatchCreate();
            }
        }
    }

    @Override
    public void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context) {
        //take care of any left over records
        executeWrite(context);

        context.getCounter("Task", "cleanup").increment(1);
    }

    private void executeWrite(org.apache.hadoop.mapreduce.Mapper.Context context)  {
        mutatorBatch.setTimestamp(3000);

        if(recordsInBulk > 0) {
            boolean wasSuccess = false;
            int retryCount = 3;
            while(wasSuccess == false && retryCount >=0) {
                try {
                    OperationResult<Void> result = mutatorBatch.execute();
                    context.getCounter("cassandra","write.Success").increment(1);
                    mutratorBatchCreate();
                    wasSuccess = true;
                } catch (Exception e) {
                    retryCount--;
                    context.getCounter("cassandra","write.Fail." + retryCount).increment(1);

                    System.out.println("Cassandra write -> " + e);
                }
            }

            if(wasSuccess == false) {
                context.getCounter("cassandra","write.Fail.Absolute").increment(1);
            }
        }
    }
    private void mutratorBatchCreate()  {
        mutatorBatch = keyspace.prepareMutationBatch();
        recordsInBulk = 0;
    }
}
