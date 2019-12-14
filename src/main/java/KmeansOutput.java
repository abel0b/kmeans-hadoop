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

public class KmeansOutput {
    public static class KmeansOutputMapper extends Mapper<Object, Text, Text, NullWritable> {
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

            context.write(value, NullWritable.get());
        }
    }
}
