package org.example;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question0_0 {
    public enum CustomCounter {
        EMPTY_LINES_COUNTER,
    }
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Map<String, Integer> wordCountMap;
        CustomCounter customCounter;
        Counter counter;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            //check if line is empty and increment empty line counter
            if(line.isEmpty()){
                counter.increment(1);
            }
            for (String stringLoop : value.toString().split(" ")) {
                stringLoop = stringLoop.replaceAll("\\s*,\\s*$", "");
                stringLoop = stringLoop.trim();
                if (wordCountMap.containsKey(stringLoop)) {
                    wordCountMap.put(stringLoop, wordCountMap.get(stringLoop) + 1);
                } else {
                    wordCountMap.put(stringLoop, 1);
                }
            }
        }

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            wordCountMap = new HashMap<>();
            counter = context.getCounter(customCounter);
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (String key : wordCountMap.keySet()) {
                context.write(new Text(key), new IntWritable(wordCountMap.get(key)));
            }
        }

    }


    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String input = otherArgs[1];
        String output = otherArgs[2];

        Job job = Job.getInstance(conf, "Question0_0");
        job.setJarByClass(Question0_0.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setCombinerClass(MyReducer.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}