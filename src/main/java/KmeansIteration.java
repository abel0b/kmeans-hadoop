import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.lang.RuntimeException;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;

public class KmeansIteration {
    public static class KmeansIterationMapper extends Mapper<Object, Text, DoubleWritable, DoubleWritable> {
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
    public static class KmeansIterationReducer
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
}
