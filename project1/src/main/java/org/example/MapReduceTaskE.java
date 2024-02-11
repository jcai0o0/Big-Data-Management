package org.example;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;


public class MapReduceTaskE {
    // this mapper will map the AccessLog dataset
    // return  Key(string): ID
    //         Value(string): PageId,1
    public static class accessMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // in AccessLog idx 1 is user and idx 2 is PageId
            String[] record = value.toString().split(",");
            context.write(new Text(record[1]), new Text(record[2] + ",1"));
        }
    }

    // this reducer will consume all the mapper output
    // return Key(String): ID
    //        Value(string): uniqueUserCount, TotalAccessCount
    public static class uniqueAccessCount extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int sumOfAccess = 0;
            // use a hashset to record unique user that this key has access
            HashSet<String> uniqueUser = new HashSet<String>();
            for (Text str:value) {
                // split into PageId and count
                String[] strValue = str.toString().split(",");
                // increment
                sumOfAccess += Integer.parseInt(strValue[1]);
                uniqueUser.add(strValue[0]);
            }
            context.write(key, new Text(sumOfAccess +","+ uniqueUser.size()));
        }
    }


    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "TaskE");
        job.setJarByClass(MapReduceTaskE.class);

        job.setMapperClass(accessMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(uniqueAccessCount.class);
        job.setReducerClass(uniqueAccessCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // path to input dataset 1 AccessLog
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, accessMapper.class);

        File file = new File(args[1]);
        FileUtils.deleteDirectory(file);
        file.delete();
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");

//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
