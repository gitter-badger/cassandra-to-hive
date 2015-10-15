package com.kenshoo.bigdata.cassandra_to_hive;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
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
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;


import java.util.Date;

/**
 * Created by noamh on 26/07/15.
 */

public class Main extends Configured implements Tool {
    public static final String CONF_CASSANDRA_ADDRESS =  "cassandraAddress";
    public static final String CONF_CASSANDRA_USERNAME = "cassandraUsername";
    public static final String CONF_CASSANDRA_PASSWORD = "cassandraPassword";
    public static final String CONF_KEYSPACE_NAME = "keyspaceName";
    public static final String CONF_COLUMN_FAMILY_NAME = "columnFamilyName";
    public static final String CONF_KEY_FIELD_NAME = "keyFieldName";
    public static final String CONF_COLUMN_NAME_FIELD_NAME = "columnNameFieldName";
    public static final String CONF_VALUE_FIELD_NAME = "valueFieldName";
    public static final String CONF_TIMESTAMP_FIELD_NAME = "timestampFieldName";
    public static final String CONF_TTL_FIELD_NAME = "ttlFieldName";
    public static final String CONF_RECORD_PER_BULK = "recordsPerBulk";
    public static final String CONF_HIVE_DATABASE = "hiveDatabase";
    public static final String CONF_HIVE_TABLE = "hiveTable";

    private Configuration conf = null;
    private CommandLine cmd = null;


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        System.out.println("Version: 0.3");
        String dbName = "", inputTableName = "";
        FileSystem fs;
        Job job = null;
        boolean configValid = false;

        //Init Job
        {
            conf = getConf();
            job = new Job(conf,Main.class.getPackage().getName());
            job.setJarByClass(Main.class);
        }

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        //Config
        {
            conf = job.getConfiguration();
            CommandLineParser parser = new GnuParser();

            //because of a limitation of .commons.cli we need to predefine all of the possible options
            Options argsOptions = new Options();

            //Options
            {
                argsOptions.addOption(CONF_CASSANDRA_ADDRESS, true, "Cassandra server address");
                argsOptions.addOption(CONF_CASSANDRA_USERNAME,true,"Cassandra Username - Optional");
                argsOptions.addOption(CONF_CASSANDRA_PASSWORD,true,"Cassandra Password - Optional");
                argsOptions.addOption(CONF_KEYSPACE_NAME,true,"Keyspace Name");
                argsOptions.addOption(CONF_COLUMN_FAMILY_NAME,true,"Column Family Name");
                argsOptions.addOption(CONF_KEY_FIELD_NAME,true,"Column Family - Key field name");
                argsOptions.addOption(CONF_COLUMN_NAME_FIELD_NAME,true,"Column Family - ColumnName field name");
                argsOptions.addOption(CONF_VALUE_FIELD_NAME,true,"Column Family  - Value field name");
                argsOptions.addOption(CONF_TIMESTAMP_FIELD_NAME,true,"Timestamp field name - Optional");
                argsOptions.addOption(CONF_TTL_FIELD_NAME,true,"ttl field name");
                argsOptions.addOption(CONF_RECORD_PER_BULK,true,"Records per insert bulk");
                argsOptions.addOption(CONF_HIVE_DATABASE,true, "hive database");
                argsOptions.addOption(CONF_HIVE_TABLE,true,"hive table");

            }

            cmd = parser.parse(argsOptions, args,true);


            if (argGetAndSetConf(CONF_CASSANDRA_ADDRESS,true) &&
                    argGetAndSetConf(CONF_CASSANDRA_USERNAME,false) &&
                    argGetAndSetConf(CONF_CASSANDRA_PASSWORD,false) &&
                    argGetAndSetConf(CONF_KEYSPACE_NAME,true) &&
                    argGetAndSetConf(CONF_COLUMN_FAMILY_NAME,true) &&
                    argGetAndSetConf(CONF_KEY_FIELD_NAME,true) &&
                    argGetAndSetConf(CONF_COLUMN_NAME_FIELD_NAME,true) &&
                    argGetAndSetConf(CONF_VALUE_FIELD_NAME,true) &&
                    argGetAndSetConf(CONF_TIMESTAMP_FIELD_NAME,true) &&
                    argGetAndSetConf(CONF_TTL_FIELD_NAME,false) &&
                    argGetAndSetConf(CONF_RECORD_PER_BULK,true)) {

                dbName = argGet(CONF_HIVE_DATABASE,true);
                inputTableName = argGet(CONF_HIVE_TABLE,true);

                if(dbName != null && dbName.isEmpty() == false && inputTableName != null && inputTableName.isEmpty() == false) {
                    configValid = true;
                } else {

                }
            }
            else {
                System.out.println("Can't continue without missing args.");
            }
        }

        if(configValid) {
            //Input
            {
                HCatInputFormat.setInput(job, dbName, inputTableName);
                job.setInputFormatClass(HCatInputFormat.class);
            }

            //Output
            {
                //Generate some temp directory
                Path outputPath = new Path("/tmp/dejavu/" + dbName + "/" + inputTableName + "/" + (new Date()).getTime());
                FileOutputFormat.setOutputPath(job, outputPath);
                FileOutputFormat.setCompressOutput(job,true);
                FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
                SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
                job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
            }
            /*
            {
                job.setOutputFormatClass(HCatOutputFormat.class);
                HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName, outputTableName,null));
                HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
                HCatOutputFormat.setSchema(job, s);
            }
            */

            //Map
            {
                //job.setMapOutputKeyClass(Text.class);
                job.setMapOutputKeyClass(BytesWritable.class);
                //job.setMapOutputValueClass(MapWritable.class);
                //AVRO
                //job.setMapOutputValueClass(AvroValue.class);
                job.setMapOutputValueClass(BytesWritable.class);
                job.setMapperClass(Map.class);
            }

            //AVRO
            //Schema avroSchema = new Schema.Parser().parse(Sandbox.cassandraSchema);
            //AvroJob.setOutputKeySchema(job,avroSchema);
            //AvroJob.setMapOutputValueSchema(job,avroSchema);


            //Reduce
            {
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setReducerClass(Reduce.class);
                job.setNumReduceTasks(15);
            }

            return (job.waitForCompletion(true) ? 0 : 1);
        } else {

            return 0;
        }

    }

    private String argGet(String keyName, boolean mustSupply) {
        if(cmd.hasOption(keyName)) {
            System.out.println(keyName  + "=" + cmd.getOptionValue(keyName));

            return cmd.getOptionValue(keyName);
        } else if (mustSupply) {
            System.out.println("Must supply " + keyName + " param");

            return null;
        }   else {
            return null;
        }
    }
    private boolean argGetAndSetConf(String keyName, boolean mustSupply) {
        return argGetAndSetConf(keyName,mustSupply,"");
    }
    private boolean argGetAndSetConf(String keyName, boolean mustSupply,String defaultValue) {
        if(cmd.hasOption(keyName)) {
            System.out.println(keyName  + "=" + cmd.getOptionValue(keyName));
            conf.set(keyName, cmd.getOptionValue(keyName));

            return true;
        } else if (mustSupply) {
            System.out.println("Must supply '" + keyName + "' param");

            return false;
        }   else {
            if(defaultValue.length() > 0)
                conf.set(keyName,defaultValue);

            return true; //value is not a must, validation passed
        }
    }
}