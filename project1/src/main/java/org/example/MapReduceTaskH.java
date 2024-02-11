//package org.example;
//
//import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//public class MapReduceTaskH {
//    public class Job2Map1 extends Mapper<LongWritable, Text, IntWritable, Text> {
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] parts = value.toString().split("\\t");
//            int id = Integer.parseInt(parts[0]);
//            context.write(new IntWritable(id), new Text("first_info"));
//        }
//    }
//
//    public class Job2Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] parts = value.toString().split(",");
//            int id = Integer.parseInt(parts[0]);
//            String name = parts[1];
//            context.write(new IntWritable(id), new Text("name_info," + name));
//        }
//    }
//
//    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String name = null;
//            boolean existsInFirstDataset = false;
//
//            for (Text val : values) {
//                String[] parts = val.toString().split(",");
//                if ("name_info".equals(parts[0])) {
//                    name = parts[1];
//                } else if ("first_info".equals(parts[0])) {
//                    existsInFirstDataset = true;
//                }
//            }
//
//            if (existsInFirstDataset && name != null) {
//                context.write(key, new Text(name));
//            }
//        }
//    }
//
//    public class RelationshipMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
//        private final IntWritable one = new IntWritable(1);
//
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] fields = value.toString().split(",");
//            for (String field : fields) {
//                int id = Integer.parseInt(field);
//                if (id != -1) {
//                    context.write(new IntWritable(id), one);
//                }
//            }
//        }
//    }
//
//    public class RelationshipReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
//        private final Map<IntWritable, Integer> relationshipCounts = new HashMap<>();
//        private int sum = 0;
//        private int count = 0;
//
//        @Override
//        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int relationships = 0;
//            for (IntWritable val : values) {
//                relationships += val.get();
//            }
//            relationshipCounts.put(new IntWritable(key.get()), relationships);
//            sum += relationships;
//            count++;
//        }
//
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//            double average = (double) sum / count;
//            for (Map.Entry<IntWritable, Integer> entry : relationshipCounts.entrySet()) {
//                if (entry.getValue() > average) {
//                    context.write(entry.getKey(), new IntWritable(entry.getValue()));
//                }
//            }
//        }
//    }
//
//}
