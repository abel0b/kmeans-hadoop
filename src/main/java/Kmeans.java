import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.lang.Math;

public class Kmeans {
    public static class KmeansMapper
    extends Mapper<Object, Text, IntWritable, IntWritable>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens[0].equals("City"))
                return;
            if (!tokens[4].isEmpty()) {
                int pop = Integer.parseInt(tokens[4]);
                context.write(
                new IntWritable(new Double(Math.log10(pop)).intValue()),
                new IntWritable(1)
                );
            }
        }
    }
    public static class KmeansReducer
    extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context
        ) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
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

        Job job = Job.getInstance(conf, "Kmeans");
        job.setNumReduceTasks(1);
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
