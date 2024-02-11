package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class sihouette {
    public static class PointMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
//            int clusterId = Integer.parseInt(tokens[0]);
            // Emitting clusterId and point (as x,y)
            context.write(new IntWritable(clusterId), new Text(tokens[0] + "," + tokens[1]));
        }
    }

    public static class IntraClusterDistanceReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        public void reduce(IntWritable clusterId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<double[]> points = new ArrayList<>();
            for (Text val : values) {
                String[] coords = val.toString().split(",");
                points.add(new double[]{Double.parseDouble(coords[0]), Double.parseDouble(coords[1])});
            }
            for (int i = 0; i < points.size(); i++) {
                for (int j = i + 1; j < points.size(); j++) {
                    double distance = Math.sqrt(Math.pow(points.get(i)[0] - points.get(j)[0], 2) + Math.pow(points.get(i)[1] - points.get(j)[1], 2));
                    context.write(new Text(clusterId.toString() + "_" + i), new DoubleWritable(distance));
                    context.write(new Text(clusterId.toString() + "_" + j), new DoubleWritable(distance));
                }
            }
        }
    }

    // ... [Mappers and Reducers for inter-cluster distances and silhouette calculations] ...

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SilhouetteScore");
        job.setJarByClass(sihouette.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(IntraClusterDistanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // path to output
        File file = new File(args[1]);
        FileUtils.deleteDirectory(file);
        file.delete();

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

