import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class CheckinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text outKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Each value is a JSON string representing a checkin record
        String line = value.toString();
        try {
            JSONObject json = new JSONObject(line);
            String checkinData = json.getString("date");
            // Split the comma-separated timestamps
            String[] timestamps = checkinData.split(",");
            for (String ts : timestamps) {
                ts = ts.trim();
                if (ts.isEmpty()) continue;
                // Extract hour from timestamp (assumes format "YYYY-MM-DD HH:MM:SS")
                String[] parts = ts.split(" ");
                if (parts.length < 2) continue;
                String timePart = parts[1];
                String hour = timePart.split(":")[0];
                outKey.set(hour);
                context.write(outKey, one);
            }
        } catch (Exception e) {
            // Log or handle malformed records if necessary
        }
    }
}
