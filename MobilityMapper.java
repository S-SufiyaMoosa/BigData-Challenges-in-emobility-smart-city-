import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobilityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text locationKey = new Text();
    private IntWritable speedValue = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split input line by comma
        String[] fields = value.toString().split(",");
        // Assuming fields: [VehicleID, Timestamp, Latitude, Longitude, Speed]
        
        if (fields.length == 5) {
            String latitude = fields[2];
            String longitude = fields[3];
            int speed = Integer.parseInt(fields[4]);  // Speed of the vehicle

            // Round latitude and longitude to 4 decimal places to aggregate close locations
            String location = String.format("%.4f", Double.parseDouble(latitude)) + "," +
                              String.format("%.4f", Double.parseDouble(longitude));

            locationKey.set(location);
            speedValue.set(speed);

            context.write(locationKey, speedValue);  // Emit (location, speed)
        }
    }
}
