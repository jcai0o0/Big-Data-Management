package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KMeansTaskD {
    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            int k = Integer.parseInt(config.get("k"));

            String[] data = value.toString().split(",");
            double cur_x = Float.parseFloat(data[0]);
            double cur_y = Float.parseFloat(data[1]);

            List<Double> res = new ArrayList<Double>();
            res.add(Double.POSITIVE_INFINITY);
            res.add((double) 1);

            for (int i = 0; i < k; i++) {
                String[] temp = config.get("C" + Integer.toString(i)).split(",");

                double temp_x = Double.valueOf(temp[0]);
                double temp_y = Double.valueOf(temp[1]);
                double distance = Math.sqrt(Math.pow(temp_x - cur_x, 2) + Math.pow(temp_y - cur_y, 2));
                if (distance < res.get(0)) {
                    res.set(0, distance);
                    res.set(1, (double)i);
                }
            }
            // output: centroid coordinate, current data point coordinate
            context.write(new Text(config.get("C" + Integer.toString(res.get(1).intValue()))), new Text(value));

        }
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            float local_sum_x = 0;
            float local_sum_y = 0;
            int count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                local_sum_x += Float.parseFloat(data[0]);
                local_sum_y += Float.parseFloat(data[1]);
                count += 1;
            }
            context.write(key, new Text(local_sum_x+","+local_sum_y+","+count));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            float global_sum_x = 0;
            float global_mean_y = 0;
            int count = 0;
            for (Text str:value) {
                String[] data = str.toString().split(",");
                global_sum_x += Float.parseFloat(data[0]);
                global_mean_y += Float.parseFloat(data[1]);
                count += Integer.parseInt(data[2]);
            }
            //output new centroid, each line indicate one centroid
            // key: x, val: y
            context.write(new Text(String.valueOf(global_sum_x/count)), new Text(String.valueOf(global_mean_y/count)));
        }
    }

    public static List<String> getRandomCentroid(String filePath, Integer k) throws Exception {
        List<String> temp = new ArrayList<String>();
        List<String> centroids = new ArrayList<String>();
        // read from local file system
//        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        // read from hdfs
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader( new InputStreamReader(inputStream));

        String line;
        while ((line = reader.readLine()) != null) {
            temp.add(line);
        }
        Random rand = new Random();
        rand.setSeed(0);
        for (int i=1; i<=k; i++) {
            int index = rand.nextInt(temp.size());
            centroids.add(temp.get(index));
        }

        return centroids;
    }

    public static List<String> getCentroids(String filePath) throws Exception {
        List<String> res = new ArrayList<String>();
        // read from local file system
//        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        // read from hdfs
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader( new InputStreamReader(inputStream));

        String line;
        while ((line = reader.readLine()) != null) {
            res.add(line);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();

        String datasetPath = args[0];
        String outputPath = args[1];
        int k = Integer.valueOf(args[2]);
        int r = Integer.valueOf(args[3]);
        double threshold = Double.valueOf(args[4]);

        String fileName = "part-r-00000";

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("k", Integer.toString(k));
        List<String> centers = new ArrayList<String>();

        boolean converge = false;
        for (int round=1; round<=r; round++) {
            if (!converge) {
                System.out.println("Current running : " + Integer.toString(round) + " round!!");
                if (round==1) {
                    centers = getRandomCentroid(datasetPath, k);
                    for (int j=0; j<k; j++) {
                        conf.set("C" + Integer.toString(j), centers.get(j));
                    }
                } else {
                    // read the updated centroids from  last iteration
                    String prevOutputFilePath = outputPath + "/Round_" + Integer.toString(round-1) + "/" + fileName;
                    centers = getCentroids(prevOutputFilePath);
                    for (int j=0; j<k; j++) {
                        conf.set("C" + Integer.toString(j), centers.get(j));
                    }
                }

                Job job = Job.getInstance(conf, "KMeansTaskD");
                job.setJarByClass(KMeansTaskD.class);

                job.setMapperClass(KMeansMapper.class);
                job.setCombinerClass(KMeansCombiner.class);
                job.setReducerClass(KMeansReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(datasetPath));

                String CurrOutputPath = outputPath + "/Round_" + Integer.toString(round) + "/";
                FileOutputFormat.setOutputPath(job, new Path(CurrOutputPath));

                boolean success = job.waitForCompletion(true);

                if (success) {
                    List<String> currentCenter = getCentroids(CurrOutputPath + "/" + fileName);
                    for (int j=0; j<k; j++) {
                        String[] prevCentroids = centers.get(j).split(",");
                        double old_x = Double.valueOf(prevCentroids[0]);
                        double old_y = Double.valueOf(prevCentroids[1]);
                        String[] currCentroids = currentCenter.get(j).split(",");
                        double new_x = Double.valueOf(currCentroids[0]);
                        double new_y = Double.valueOf(currCentroids[1]);
                        double diff_x = Math.abs(new_x - old_x);
                        double diff_y = Math.abs(new_y - old_y);
                        if (diff_x <= threshold && diff_y <= threshold){
                            converge = true;
                        } else {
                            converge = false;
                            break;
                        }
                    }
                } else {
                    System.out.println("MapReduce logic error");
                }

            } else {
                System.out.println("Successfully converged at " + Integer.toString(round-1) + " round!!");
                break;
            }
        }
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println("Task finished in " + seconds + " seconds");
    }



}
