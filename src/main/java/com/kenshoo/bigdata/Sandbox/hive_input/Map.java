package com.kenshoo.bigdata.Sandbox.hive_input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * Created by noamh on 30/07/15.
 */
public class Map extends Mapper<WritableComparable, HCatRecord, Text, Text> {

    @Override
    public void map(WritableComparable key, HCatRecord value,
                    org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord, Text, Text>.Context context) {
        context.getCounter("Map.Custom","GotRecord").increment(1);
    }
}
