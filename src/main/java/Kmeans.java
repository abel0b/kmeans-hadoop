import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import java.lang.Math;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.io.SequenceFile;

public class Kmeans {
    public static void main(String[] args) throws Exception, IOException {
        if (args.length < 4 || args.length > 4) {
            System.err.println("[error] Invalid arguments");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        conf.setInt("k", Integer.parseInt(args[2]));
        conf.setInt("column", Integer.parseInt(args[3]));
        conf.set("centroidsPath", String.format("%s-centroids", args[1]));

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centroids = new Path(conf.get("centroidsPath"));

        FileSystem fs = FileSystem.get(conf);
        
        if(fs.exists(centroids)) {
            fs.delete(centroids);
        }

        if(fs.exists(output)) {
            fs.delete(output);
        }

        Initializer initializer = new RandomInitializer();
        initializer.initialize(conf);
        initializer.saveToFile(conf, centroids);

        System.out.printf("Initialization done\n");

        boolean converged = false;
        int iteration = 0;
        while(!converged) {
            iteration ++;
            String jobname = String.format("kmeans-iteration-%d", iteration);
            Job job = Job.getInstance(conf, jobname);
            job.setNumReduceTasks(1);
            job.setJarByClass(KmeansIteration.class);
            job.setMapperClass(KmeansIteration.KmeansIterationMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setReducerClass(KmeansIteration.KmeansIterationReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);
            
            if(!job.waitForCompletion(true)) {
                System.err.printf("Job [%s] failed\n", jobname);
                System.exit(1);
            }
            
            fs.delete(centroids);
            fs.rename(new Path(String.format("%s/part-r-00000", output.toString())), centroids);
            fs.delete(output);
            converged = iteration == 10;
        }

        System.out.printf("K-means converged after %d iterations\n", iteration);

        String outputJobname = "kmeans-output";
        Job job = Job.getInstance(conf, outputJobname);
        job.setNumReduceTasks(1);
        job.setJarByClass(KmeansOutput.class);
        job.setMapperClass(KmeansOutput.KmeansOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        if(!job.waitForCompletion(true)) {
            System.err.printf("Job [%s] failed\n", outputJobname);
            System.exit(1);
        }
        
        System.out.printf("Output file written in %s\n", args[1]);
    }
}
