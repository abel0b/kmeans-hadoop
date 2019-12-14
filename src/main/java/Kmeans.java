import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

public class Kmeans {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Missing arguments");
            System.exit(1);
        }
        if (args.length > 4) {
            System.out.println("Too much arguments");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.setInt("k", Integer.parseInt(args[2]));
        conf.setInt("column", Integer.parseInt(args[3]));

        Path hdfsInputPath = new Path(args[0]);
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(hdfsInputPath);
        BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(inputStream, StandardCharsets.UTF_8)
        );

        String line = null;

        String centroids = "";
        for (int i =0; i<conf.getInt("k", 42);i++) {
            if (i != 0) {
                centroids = centroids.concat(" ");
            }
            line = bufferedReader.readLine();
            System.out.println(line);
            centroids = centroids.concat(String.valueOf(Math.random()));
        }
        conf.setStrings("centroids", centroids);

        System.out.printf("okok\n");

        boolean converged = false;
        int iteration = 0;
        while(!converged) {
            iteration ++;
            String jobname = String.format("k-means iteration %d", iteration);
            Job job = Job.getInstance(conf, jobname);
            job.setNumReduceTasks(1);
            job.setJarByClass(KmeansIteration.class);
            job.setMapperClass(KmeansIteration.KmeansIterationMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setReducerClass(KmeansIteration.KmeansIterationReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            if(!job.waitForCompletion(true)) {
                System.err.printf("Job [%s] failed\n", jobname);
                System.exit(1);
            }
            converged = iteration == 3;
        }

        System.out.printf("K-means converged after %d iterations", iteration);

        Job job = Job.getInstance(conf, "k-means output");
        job.setNumReduceTasks(1);
        job.setJarByClass(KmeansOutput.class);
        job.setMapperClass(KmeansOutput.KmeansOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        System.out.printf("Output file written in %s\n", args[1]);
    }
}
