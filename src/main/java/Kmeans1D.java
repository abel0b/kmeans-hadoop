import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.lang.Math;
import java.lang.RuntimeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class Kmeans1D {
    public static class Kmeans1DMapper
    extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
        int k;
        int column;
        double centroids[];

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 42);
            column = conf.getInt("column", 42);
            
            String input_centroids[] = conf.getStrings("centroids")[0].split(" ");
            centroids = new double[k];
            for(int i=0; i<k; i++) {
                centroids[i] = Double.valueOf(input_centroids[i]);
            }
            System.out.println(k);
            System.out.println(column);
        } 

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, RuntimeException {
            
            String token = value.toString().split(",")[column];
            
            if (token.isEmpty()) return;
            
                        
            Double point = Double.valueOf(token);

            double centroid = centroids[0];
            double dmin = Math.abs(point - centroids[0]);
            for(int i=1; i<k; i++) {
                double dtmp = Math.abs(point - centroids[i]);
                if (dtmp < dmin) {
                    dmin = dtmp;
                    centroid = centroids[i];
                }
            }

            context.write(new DoubleWritable(centroid), new DoubleWritable(point));
        }
    }
    public static class Kmeans1DReducer
    extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,NullWritable> {
        int k;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 42); 
        }
        
        public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double mean = 0.0;

            int nbp = 0;
            for(DoubleWritable point: values) { 
                mean = mean + point.get();
                nbp++;
            }
            mean = mean / Double.valueOf(nbp);
            context.write(new DoubleWritable(mean), NullWritable.get());
        }
    }
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

        Job job = Job.getInstance(conf, "Kmeans1D");
        job.setNumReduceTasks(1);
        job.setJarByClass(Kmeans1D.class);
        job.setMapperClass(Kmeans1DMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setReducerClass(Kmeans1DReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
