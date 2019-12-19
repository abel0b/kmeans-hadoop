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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;

public class KmeansOutput {
    public static class KmeansOutputMapper extends Mapper<Object, Text, Text, NullWritable> {
        int k;
        int column;
        double centroids[];

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 42);
            column = conf.getInt("column", 42);
            
            Path centroidsPath = new Path(conf.get("centroidsPath"));
           
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(
                    conf,
                    SequenceFile.Reader.file(centroidsPath)
                );
            
                DoubleWritable centroid = new DoubleWritable();

                centroids = new double[k];
                int i = 0;
                while(reader.next(centroid, NullWritable.get())) {
                    centroids[i] = centroid.get();
                    i++;
                }
                reader.close();
            }
            catch(IOException e) {
                System.err.println("Could not read centroid file");
                System.exit(1);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, RuntimeException {

            String token = value.toString().split(",")[column];
            
            if (token.isEmpty()) return;
            Double point = Double.valueOf(token);

            int cluster = 1;
            double dmin = Math.abs(point - centroids[0]);
            for(int i=1; i<k; i++) {
                double dtmp = Math.abs(point - centroids[i]);
                if (dtmp < dmin) {
                    dmin = dtmp;
                    cluster = i+1;
                }
            }
            String outputLine = String.format("%s,%d", value.toString(), cluster);
            context.write(new Text(outputLine), NullWritable.get());
        }
    }
}
