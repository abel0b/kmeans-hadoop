import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.lang.Math;

public class RandomInitializer extends Initializer { 
    @Override
    void initialize(Configuration conf) {
        for (int i=0; i<conf.getInt("k", 42); i++) {        
            this.centroids.add(new Centroid(Math.random()));
        }
    }
}
