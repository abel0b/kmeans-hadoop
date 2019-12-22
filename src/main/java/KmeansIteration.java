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
import org.apache.hadoop.mapred.Reporter;
import java.lang.NumberFormatException;

public class KmeansIteration {
    public static class KmeansIterationMapper extends Mapper<Object, Text, IntWritable, Point> {
        int k;
        int n;
        int iteration;
        int columns[];
        Point centroids[];

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("k", 42);
            this.n = conf.getInt("n", 42);
            this.iteration = conf.getInt("iteration", 42);

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
                if(i!=this.k) throw new RuntimeException(String.format("Incomplete centroids file %d/%d\n", i, this.k));
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
            double dmin = point.squared_dist(centroids[0]);
            for(int i=1; i<this.k; i++) {
                // Squared distance for faster computation
                double dtmp = point.squared_dist(centroids[i]);
                if (dtmp < dmin) {
                    dmin = dtmp;
                    cluster = i+1;
                }
            }

            context.write(new IntWritable(cluster), point);
        }
    }
    
    public static class KmeansIterationReducer
    extends Reducer<IntWritable,Point,IntWritable,Point> {
        int k;
        int n;
        Point centroids[];

        static enum ConvergenceCounter {
            CONVERGED
        };

        static double epsilon = 0.1;

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("k", 42);
            this.n = conf.getInt("n", 42);
            Path centroidsPath = new Path(conf.get("centroidsPath"));

            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(
                    conf,
                    SequenceFile.Reader.file(centroidsPath)
                );
                Point centroid = new Point();
                IntWritable cluster = new IntWritable();
                this.centroids = new Point[k];
                int i = 0;
                while(reader.next(cluster, centroid)) {
                    this.centroids[i] = centroid;
                    centroid = new Point();
                    i++;
                }
                if(i!=this.k) throw new RuntimeException(String.format("Incomplete centroids file %d/%d\n", i, this.k));
                reader.close();
            }
            catch(IOException e) {
                throw e;
            }
        }

        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            // Compute new centroid
            List<DoubleWritable> mean = new ArrayList();
            for(int i=0;i<this.n;i++) {
                mean.add(new DoubleWritable(0.0));
            }

            int nbp = 0;
            for(Point point: values) {
                for(int i=0; i<this.n; i++) {
                    mean.set(i, new DoubleWritable(mean.get(i).get() + point.coordinates.get(i).get()));
                }
                nbp++;
            }
            
            for(int i=0;i<this.n;i++) {
                mean.set(i, new DoubleWritable(mean.get(i).get() / Double.valueOf(nbp)));
            }

            Point centroid = new Point(mean);
            
            // Increment CONVERGED counter
            if (this.centroids[key.get()-1].squared_dist(centroid) < this.epsilon) {
                context.getCounter(ConvergenceCounter.CONVERGED).increment(1);
            }
            context.write(new IntWritable(key.get()), centroid);
        }
    }
}
