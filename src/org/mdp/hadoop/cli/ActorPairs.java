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

public class ActorPairs {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if( otherArgs.length != 2 ) {
            System.err.println("Usage: "+ ActorPairs.class.getName() + " <in><out>" );
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String intermediateOutputLocation1 = otherArgs[1]+"/temp";
        String intermediateOutputLocation2 = otherArgs[1]+"/temp2";
        String outputLocation = otherArgs[1]+"/final";

        Job job1 = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job1, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job1, new Path(intermediateOutputLocation1));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setMapperClass(MovieActorPairer.MovieActorPairrerMapper.class);
        job1.setReducerClass(MovieActorPairer.MovieActorPairrerReducer.class);

        job1.setJarByClass(MovieActorPairer.class);
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job2, new Path(intermediateOutputLocation1));
        FileOutputFormat.setOutputPath(job2, new Path(intermediateOutputLocation2));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setMapperClass(ActorPairCounter.ActorPairCounterMapper.class);
        job2.setReducerClass(ActorPairCounter.ActorPairCounterReducer.class);

        job2.setJarByClass(ActorPairCounter.class);
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job3, new Path(intermediateOutputLocation2));
        FileOutputFormat.setOutputPath(job3, new Path(outputLocation));

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setMapperClass(SortWordCounts.SortWordCountsMapper.class);
        job3.setReducerClass(SortWordCounts.SortWordCountsReducer.class);

        job3.setJarByClass(ActorPairCounter.class);
        job3.waitForCompletion(true);

    }
}
