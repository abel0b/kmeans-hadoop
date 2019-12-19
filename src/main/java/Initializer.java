import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;

public abstract class Initializer {
    List<Centroid> centroids;

    Initializer() {
        this.centroids = new ArrayList();
    }
    
    abstract void initialize(Configuration conf);

    public void saveToFile(Configuration conf, Path output) throws IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(
            conf,
            SequenceFile.Writer.file(output),
            SequenceFile.Writer.keyClass(DoubleWritable.class),
            SequenceFile.Writer.valueClass(NullWritable.class)
        );

        ListIterator<Centroid> centroidIterator = this.centroids.listIterator();
        int i = 0;
        while(centroidIterator.hasNext()) {
            writer.append(
                new DoubleWritable(centroidIterator.next().get()),
                NullWritable.get()
            );
            i++;
        }

        writer.close();
    }
}
