import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.Text;
import java.lang.Exception;
import java.util.concurrent.ThreadLocalRandom;
import java.lang.NumberFormatException;

public class Initializer {
    private List<Point> centroids;

    Initializer(Configuration conf) throws Exception {
        this.centroids = new ArrayList();
        this.initialize(conf);
    }
    
    // Initialize centroids with random points chosen in data
    void initialize(Configuration conf) throws Exception {
        int k = conf.getInt("k", 42);
        int n = conf.getInt("n", 42);
        
        int columns[] = new int[n];
        int i = 0;
        for(String column: conf.get("columns").split(",")) {
            columns[i] = Integer.parseInt(column);
            i++;
        }
        
        try {
            Path input = new Path(conf.get("inputPath"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(input)));
            
            String line;
            line = br.readLine();

            int n_centroids = 0;
            int lineNumber = 1;
            // Chose random line in input text using reservoir sampling algorithm
            while (line != null) {
                String tokens[] = line.split(",");
                boolean valid = true;
                List<DoubleWritable> coordinates = new ArrayList();
                for(int j=0;j<n;j++) {
                    valid = valid && !tokens[columns[j]].isEmpty();
                    if(!valid) break;
                    coordinates.add(new DoubleWritable(Double.valueOf(tokens[columns[j]])));
                }
                if(!valid) break; // Ignore line with missing data
                
                if(n_centroids < k) {
                    this.centroids.add(new Point(coordinates));
                    n_centroids ++;
                }
                else {               
                    int j = ThreadLocalRandom.current().nextInt(0, lineNumber);
                    if (j < k) {
                        this.centroids.set(j, new Point(coordinates));
                    }
                }

               line = br.readLine();
               lineNumber ++;
            }
        }
        catch(Exception e){
            throw e;
        }
    }

    // Save centroids to file
    public void saveToFile(Configuration conf, Path output) throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(
            conf,
            SequenceFile.Writer.file(output),
            SequenceFile.Writer.keyClass(IntWritable.class),
            SequenceFile.Writer.valueClass(Point.class)
        );

        ListIterator<Point> centroidIterator = this.centroids.listIterator();
        int cluster = 0;
        while(centroidIterator.hasNext()) {
            cluster++;
            writer.append(
                new IntWritable(cluster),
                centroidIterator.next()
            );
        }

        writer.close();
    }
}
