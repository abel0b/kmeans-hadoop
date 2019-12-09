import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (k != 3) {
                throw new RuntimeException("Bad.");  
            }
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
    extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable> {
        int k;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 42); 
        }
        
        public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double mean = 0.0;
            
            int nbp = 0;
            for(DoubleWritable point: values) {
                nbp++;
            }

            double points[] = new double[nbp];
            int i = 0;
            for(DoubleWritable point: values) {
                mean += point.get();
                points[i] = point.get();
                i++;
            }
            mean = mean / Double.valueOf(nbp);
            context.write(new DoubleWritable(mean), new DoubleWritable(1.0));
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

        String centroids = "";
        for (int i =0; i<conf.getInt("k", 42);i++) {
            if (i != 0) {
                centroids = centroids.concat(" ");
            }
            centroids = centroids.concat(String.valueOf(Math.random() * 10000.0));
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
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
