import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// N-dimensional Point Class
public class Point implements WritableComparable<Point> {
    public List<DoubleWritable> coordinates;

    public Point() {
        this.coordinates = new ArrayList();
    }

    public Point(List<DoubleWritable> coordinates) {
        this.coordinates = coordinates;
    }

    // Squared euclidian distance between 2 points
    public double squared_dist(Point other) {
        double dist = 0.0;
        for(int i=0; i<this.coordinates.size();i++) {
            dist = dist + (this.coordinates.get(i).get()-other.coordinates.get(i).get())*(this.coordinates.get(i).get()-other.coordinates.get(i).get());
        }
        return dist;
    }

    public void readFields(DataInput input) throws IOException {
        int n = input.readInt();
        this.coordinates.clear();
        for (int i=0; i<n; i++) {
            this.coordinates.add(new DoubleWritable(input.readDouble()));
        }
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(this.coordinates.size());
        for (DoubleWritable coordinate: this.coordinates) {
            output.writeDouble(coordinate.get());
        }
    }

    public int compareTo(Point other) {
        return 0;
    }

    /*public String toString() {
        String out = "Point(";
        boolean first_item = true;;
        for(DoubleWritable coordinate: this.coordinates) {
            if(first_item) {
                first_item = false;
            }
            else {
                out = out.concat(", ");
            }
            out = out.concat(String.valueOf(coordinate.get()));
        }
        out = out.concat(")");
        return out;
    }*/
}
