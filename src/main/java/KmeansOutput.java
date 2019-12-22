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
import java.util.List;
import java.util.ArrayList;

public class KmeansOutput {
    public static class KmeansOutputMapper extends Mapper<Object, Text, Text, NullWritable> {
        int k;
        int n;
        int columns[];
        Point centroids[];

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("k", 42);
            this.n = conf.getInt("n", 42);
            
            this.columns = new int[n];
            int i = 0;
            for(String column: conf.get("columns").split(",")) {
                this.columns[i] = Integer.parseInt(column);
                i++;
            }

            Path centroidsPath = new Path(conf.get("centroidsPath"));
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(
                    conf,
                    SequenceFile.Reader.file(centroidsPath)
                );
            
                Point centroid = new Point();
                IntWritable cluster = new IntWritable();
                this.centroids = new Point[k];
                i = 0;
                while(reader.next(cluster, centroid)) {
                    this.centroids[i] = centroid;
                    centroid = new Point();
                    i++;
                }
                reader.close();
            }
            catch(IOException e) {
                throw e;
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, RuntimeException {

            String tokens[] = value.toString().split(",");

            List<DoubleWritable> coordinates = new ArrayList();
            for(int i=0; i<this.n; i++) {
                if (tokens[this.columns[i]].isEmpty()) return;
                coordinates.add(new DoubleWritable(Double.valueOf(tokens[this.columns[i]])));
            }
            Point point = new Point(coordinates);

            int cluster = 1;
            double dmin = point.squared_dist(this.centroids[0]);
            for(int i=1; i<k; i++) {
                double dtmp = point.squared_dist(this.centroids[i]);
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
