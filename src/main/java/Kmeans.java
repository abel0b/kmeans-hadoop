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
        if (args.length < 4) {
            System.err.println("Missing arguments");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        final int max_iteration = 100;
        conf.setInt("k", Integer.parseInt(args[2]));
        
        String columns = "";
        int i;
        for(i=3; i<args.length; i++) {
            if(i != 3) {
                columns = columns.concat(",");
            }
            Integer.parseInt(args[i]);
            columns = columns.concat(args[i]);
        }
        
        conf.setInt("n", i-3);;
        conf.set("columns", columns);
        conf.set("centroidsPath", String.format("%s-centroids", args[1]));
        conf.set("inputPath", args[0]);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tmpPath = new Path(String.format("%s-tmp", args[1]));
        Path centroidsPath = new Path(conf.get("centroidsPath"));

        FileSystem fs = FileSystem.get(conf);
        
        if (fs.exists(centroidsPath)) {
            fs.delete(centroidsPath);
        }

        if (fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        if (fs.exists(tmpPath)) {
            fs.delete(tmpPath);
        }

        Initializer initializer = new Initializer(conf);
        initializer.saveToFile(conf, centroidsPath);

        System.out.printf("Initialization done\n");

        boolean converged = false;
        int iteration = 0;
        while (!converged && iteration < max_iteration) {
            iteration ++;
            conf.setInt("iteration", iteration);
            String jobname = String.format("KmeansIteration(%d)", iteration);
            Job job = Job.getInstance(conf, jobname);
            job.setNumReduceTasks(1);
            job.setJarByClass(KmeansIteration.class);
            job.setMapperClass(KmeansIteration.KmeansIterationMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            job.setReducerClass(KmeansIteration.KmeansIterationReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, tmpPath);
            
            if(!job.waitForCompletion(true)) {
                System.err.printf("Job [%s] failed\n", jobname);
                System.exit(1);
            }
            
            fs.delete(centroidsPath);
            fs.rename(new Path(String.format("%s/part-r-00000", tmpPath.toString())), centroidsPath);
            fs.delete(tmpPath);
            long nbconverged = job.getCounters().findCounter(KmeansIteration.KmeansIterationReducer.ConvergenceCounter.CONVERGED).getValue();
            converged = nbconverged == conf.getInt("k", 42);
            System.out.println(String.format("converged = %d/%d", nbconverged, conf.getInt("k", 42)));
        }

        System.out.printf("K-means converged after %d iterations\n", iteration);

        String outputJobname = "KmeansOutput";
        Job job = Job.getInstance(conf, outputJobname);
        job.setNumReduceTasks(1);
        job.setJarByClass(KmeansOutput.class);
        job.setMapperClass(KmeansOutput.KmeansOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, tmpPath);
        
        if (!job.waitForCompletion(true)) {
            System.err.printf("Job [%s] failed\n", outputJobname);
            System.exit(1);
        }

        fs.rename(new Path(String.format("%s/part-r-00000", tmpPath.toString())), outputPath);
        fs.delete(tmpPath);
        
        System.out.printf("Output file written in %s\n", outputPath.toString());
    }
}
