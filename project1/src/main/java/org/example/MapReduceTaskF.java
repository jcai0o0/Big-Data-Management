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
import java.util.HashMap;
import java.util.Map;


public class MapReduceTaskF {
    // this mapper will map the AccessLog dataset
    // return  Key(string): ByWho(PersonA ID)
    //         Value(string): "WhatPage(PersonB ID),1"
    public static class accessMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // in AccessLog idx 1 is user and idx 2 is PageId
            String[] record = value.toString().split(",");
            context.write(new Text(record[1]), new Text(record[2] + ",1"));
        }
    }

    // this mapper will map the Associate dataset
    // return  Key(string): PersonaA ID
    //         Value(string): "PersonB ID,0"
    public static class friendMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            context.write(new Text(record[1]), new Text(record[2]+",0"));
            context.write(new Text(record[2]), new Text(record[1]+",0"));

        }
    }

    // this mapper will map the FaceInPage dataset, to join the output from fakeFriendFinder
    // return  Key(string): ID
    //         Value(string): "Name,-1"
    public static class nameMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            context.write(new Text(record[0]), new Text(record[1]+",-1"));
        }
    }

    // this reducer will consume all the mapper output
    // for all mapper return data
    // split into a string array and use first string as key to save into a hashset
    // and use second string as value to count frequency
    // if the value in the final hashset is 0 means this is a friend but have not been accessed
    public static class fakeFriendFinder extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> counter = new HashMap<String, Integer>();
            String name = "null";
            for (Text str:value) {
                String[] strValue= str.toString().split(",");
                if (strValue[1].equals("-1")) {
                    name = strValue[0];
                } else {
                    if (counter.containsKey(strValue[0])) {
                        int currentCount = counter.get(strValue[0]);
                        counter.put(strValue[0], currentCount + Integer.parseInt(strValue[1]));
                    } else {
                        counter.put(strValue[0], Integer.parseInt(strValue[1]));
                    }
                }
            }
            for (Map.Entry<String , Integer> mapElement: counter.entrySet()) {
                int tempValue = mapElement.getValue();
                if (tempValue==0) {
                    context.write(key, new Text(name));
                    break;
                }
            }

//            StringBuilder ans = new StringBuilder();
//
//            for (Map.Entry<String , Integer> mapElement: counter.entrySet()) {
//                int tempValue = mapElement.getValue();
//                if (tempValue==0) {
//                    ans.append(tempValue + ",");
//                }
//            }
//
//            ans.deleteCharAt(ans.length()-1);
//            context.write(new Text(key + "," + name), new Text(ans.toString()));

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "TaskF");
        job.setJarByClass(MapReduceTaskE.class);

        job.setMapperClass(accessMapper.class);
        job.setMapperClass(friendMapper.class);
        job.setMapperClass(nameMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(fakeFriendFinder.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // path to input dataset 1 AccessLog
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, accessMapper.class);
        // path to input dataset 1 Associate
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, friendMapper.class);
        // path to input dataset 2 FaceInPage
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, nameMapper.class);


        File file = new File(args[3]);
        FileUtils.deleteDirectory(file);
        file.delete();
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
