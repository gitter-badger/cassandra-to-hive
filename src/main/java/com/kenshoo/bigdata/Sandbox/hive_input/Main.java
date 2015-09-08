package com.kenshoo.bigdata.Sandbox.hive_input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

/**
 * Created by noamh on 30/07/15.
 */
public class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String dbName, inputTableName,outputTableName;
        Job job = null;

        Path outputPath;
        String outputDirectory;

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        //User Input
        {
            dbName = args[0];
            System.out.println("Database: " + dbName);

            inputTableName = args[1];
            System.out.println("Input Table: " + inputTableName);

            outputDirectory = args[2];
            System.out.println("Output Directory: " + outputDirectory);
            outputPath = new Path(outputDirectory);

        }

        //Init Job
        {
            job = new Job(conf,Main.class.getPackage().getName());
            job.setJarByClass(Main.class);
        }

        //Input
        {
            HCatInputFormat.setInput(job, dbName, inputTableName);
            job.setInputFormatClass(HCatInputFormat.class);
        }

        //Delete output path
        {
            final FileSystem fs = outputPath.getFileSystem(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
        }

        //Output
        {
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        }

        //Map
        {
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DefaultHCatRecord.class);
            job.setMapperClass(Map.class);
        }

        //Reduce
        {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(HCatRecord.class);
        }


        return (job.waitForCompletion(true) ? 0 : 1);
    }
}