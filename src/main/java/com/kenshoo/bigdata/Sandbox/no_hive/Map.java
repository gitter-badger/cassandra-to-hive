package com.kenshoo.bigdata.Sandbox.no_hive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by noamh on 30/07/15.
 */
public class Map extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value,
                    org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context) {
    }
}