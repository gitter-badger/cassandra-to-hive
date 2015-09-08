package com.kenshoo.bigdata.cassandra_to_hive;

/*
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Using;
import com.datastax.driver.core.schemabuilder.UDTType;
*/

import javax.management.Query;
import java.util.UUID;

/**
 * Created by noamh on 19/07/15.
 */
public class Sandbox {
    public static void main(String[] args) {
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
}
