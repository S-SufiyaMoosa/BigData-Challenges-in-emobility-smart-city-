import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CongestionReducer extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sumSpeed = 0;
        int count = 0;
        
        // Calculate the total speed and the number of vehicles at the location
        for (IntWritable value : values) {
            sumSpeed += value.get();
            count++;
        }

        // Calculate the average speed at the location
        double avgSpeed = sumSpeed / (double) count;

        // Determine if the location is experiencing traffic congestion (e.g., avgSpeed < 20 km/h)
        String status = (avgSpeed < 20) ? "Congested" : "Normal";

        // Emit (location, "Congested"/"Normal" + avgSpeed)
        context.write(key, new Text(status + "\t" + avgSpeed));
    }
}
