package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;


public class MapReduceTaskD {
    // this mapper will map the FaceInPage dataset
    // return  Key(string): ID
    //         Value(string): Name,0
    public static class nameMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            context.write(new Text(record[0]), new Text(record[1]+",0"));
        }
    }

    // this mapper will map the Associates dataset
    // e.g: when we have A->B in Associates dataset
    //      we will save 2 entry to count both A and B
    // return  Key(string): ID
    //         Value(string): null,1
    public static class friendMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(",");
            context.write(new Text(record[1]), new Text("null,1"));
            context.write(new Text(record[2]), new Text("null,1"));
        }
    }

    // this reducer will consume all the mapper output
    // pretty much all the mapper will have Name,Increment
    // for FaceInPage, it will be Name,0
    // for Associate, it will be null,1
    // Key(string) ID
    // Value: [Name,0], [null, 1], .....
    public static class SumRelationship extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            int sumOfRelationship = 0;
            String name = "null";
            for (Text str:value) {
                // split into Name and Increment
                String[] str_value = str.toString().split(",");
                // if we encounter value from FaceInPage, update name with it
                name = (str_value[0].equals("null")) ? name:str_value[0];
                // increment
                sumOfRelationship += Integer.parseInt(str_value[1]);
            }
            context.write(new Text(name), new Text(Integer.toString(sumOfRelationship)));
        }
    }


    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(MapReduceTaskD.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SumRelationship.class);

        job.setCombinerClass(SumRelationship.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // path to input dataset 1 FaceInPage
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, nameMapper.class);
        // path to input dataset 2 Associate
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, friendMapper.class);

        // path to output
        File file = new File(args[2]);
        FileUtils.deleteDirectory(file);
        file.delete();

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");

//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
