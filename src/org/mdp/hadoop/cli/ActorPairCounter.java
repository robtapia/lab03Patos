package org.mdp.hadoop.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// map1 = genera <pelicula , actor>
// reduce1 = genera <pelicula, actor1 ## actor 2>

public class ActorPairCounter {

    public static String SPLIT_REGEX = "[^\\p{L}]+";

    public static class ActorPairCounterMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);


        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            output.write(new Text(split[1]), one);
        }
    }

    public static class ActorPairCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context output) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            output.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: " + ActorPairCounter.class.getName() + " <in> <out>");
                System.exit(2);
            }
            String inputLocation = otherArgs[0];
            String outputLocation = otherArgs[1];

            Job job = Job.getInstance(new Configuration());

            FileInputFormat.setInputPaths(job, new Path(inputLocation));
            FileOutputFormat.setOutputPath(job, new Path(outputLocation));

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setMapperClass(ActorPairCounter.ActorPairCounterMapper.class);
            job.setReducerClass(ActorPairCounter.ActorPairCounterReducer.class);

            job.setJarByClass(ActorPairCounter.class);
            job.waitForCompletion(true);
        }
}
