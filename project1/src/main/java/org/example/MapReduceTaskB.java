package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class MapReduceTaskB {
    // this mapper will map the AccessLog dataset
    // return  Key(string): WhatPage(PersonB ID)
    //         Value(string): 1
    public static class accessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // in AccessLog idx 1 is user and idx 2 is PageId
            String[] record = value.toString().split(",");
            context.write(new Text(record[2]), new IntWritable(1));
        }
    }

    public static class popularityCal extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sumOfRelationship = 0;
            for (IntWritable tmpCount:value) {
                // increment
                sumOfRelationship += tmpCount.get();
            }
            context.write(key, new IntWritable(sumOfRelationship));
        }
    }

    // this mapper will map from previous job output
    public static class popularityMapper extends Mapper<Text, IntWritable, Text, Text> {
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            context.write(new Text("All"), new Text(key+","+record));
        }
    }

    public static class popularityTopN extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, ArrayList<String>> sortedCounter = new TreeMap<Integer, ArrayList<String>>();
            for (Text str:value) {
                String[] record = str.toString().split(",");
                String id = record[0];
                int count = Integer.parseInt(record[1]);

                if (sortedCounter.containsKey(count)) {
                    sortedCounter.get(count).add(id);
                } else {
                    ArrayList<String> tmp = new ArrayList<String>();
                    tmp.add(id);
                    sortedCounter.put(count, tmp);
                }
            }
            int count = 0;
            for (Map.Entry<Integer , ArrayList<String>> mapElement: sortedCounter.entrySet()) {
                for (String id:mapElement.getValue()){
                    context.write(new Text(id), new IntWritable(mapElement.getKey()));
                    count+=1;
                    if (count >= 10) {
                        break;
                    }
                }
                if (count >= 10) {
                    break;
                }
            }
        }
    }



    public static void main(String[] args) throws Exception {
        // MapReduce Job 1
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "TaskB");
        job.setJarByClass(MapReduceTaskB.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(popularityCal.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // path to input dataset 1 AccessLog
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, accessMapper.class);

        // path to output
        File file = new File(args[2]+"Temp");
        FileUtils.deleteDirectory(file);
        file.delete();

        FileOutputFormat.setOutputPath(job, new Path(args[2]+"Temp1"));

        job.waitForCompletion(true);

        // MapReduce Job 2
        Configuration conf2 = new Configuration();
        conf2.set("mapred.textoutputformat.separator", ",");
        Job job2 = Job.getInstance(conf2, "TaskB");
        job2.setJarByClass(MapReduceTaskB.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(popularityTopN.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // path to input from job 1 output
        MultipleInputs.addInputPath(job2, new Path(args[2]+"Temp1"), TextInputFormat.class, popularityMapper.class);

        // path to output
        File file2 = new File(args[2]);
        FileUtils.deleteDirectory(file2);
        file2.delete();

        FileOutputFormat.setOutputPath(job2, new Path(args[2]+"Temp2"));





        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

