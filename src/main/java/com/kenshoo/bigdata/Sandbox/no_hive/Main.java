package com.kenshoo.bigdata.Sandbox.no_hive;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by noamh on 30/07/15.
 */
public class Main extends Configured implements Tool {

    static Configuration conf;
    static FileSystem fs;


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        conf = new Configuration();

        Path outputPath,inputPath;
        String inputDirectory,outputDirectory;

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        inputDirectory = args[0];
        outputDirectory = args[1];


        //Init
        {
            fs = FileSystem.get(URI.create("/"),conf);
            outputPath = new Path(outputDirectory);
            inputPath = new Path(inputDirectory);
        }

        Job job = new Job(conf,"CassMigration.ImportIndexLookupOrder");
        job.setJarByClass(Main.class);

        //Input
        {
            job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
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
            FileOutputFormat.setCompressOutput(job,true);
            FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
        }

        //Map
        {
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapperClass(Map.class);
        }

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
