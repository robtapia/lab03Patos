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

// map1 = genera <pelicula , actor>
// reduce1 = genera <pelicula, actor1 ## actor 2>

public class MovieActorPairer {

    public static String SPLIT_REGEX = "[^\\p{L}]+";

    public static class MovieActorPairrerMapper extends Mapper<Object, Text, Text, Text>{

        private Text peliculaText = new Text();
        private Text actorText= new Text();

        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {
            String line = value.toString();
            // line == starname moviename   ....
            String[] rawWords = line.split(SPLIT_REGEX); // separa linea por linea
            String[] lineaSpliteada;

            String pelicula;
            String actor;
            String parAnterior = "BASURAPARAEMPEZARELFOR";
            for(String rawWord:rawWords) {
                // rawWord es una linea en particular con el formato starname ... []
                lineaSpliteada = line.split("\t");
                if( !rawWord.isEmpty() ) {
                    if( lineaSpliteada[4].equals( "THEATRICAL_MOVIE" ) ) {
                        pelicula = lineaSpliteada[1] + "##" + lineaSpliteada[2] + "##" + lineaSpliteada[3];
                        actor = lineaSpliteada[0];

                        pelicula = pelicula.toLowerCase();
                        actor = actor.toLowerCase();
                        peliculaText.set(pelicula);
                        actorText.set(actor);
                        if( !(pelicula+actor).equals(parAnterior) ) {
                            output.write(peliculaText, actorText);
                            parAnterior = pelicula + actor;
                        }

                    }
                }
            }
        }
    }
    public static class MovieActorPairrerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context output) throws IOException, InterruptedException {

            for(Text value:values) {

                    output.write(key,value);


            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: "+CountWords.class.getName()+" <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MovieActorPairer.MovieActorPairrerMapper.class);
        job.setReducerClass(MovieActorPairer.MovieActorPairrerReducer.class);

        job.setJarByClass(MovieActorPairer.class);
        job.waitForCompletion(true);
    }
}


