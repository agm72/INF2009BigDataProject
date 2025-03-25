import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        int totalBusinesses = 0;
        int totalReviews = 0;
        double sumStars = 0.0;
        int totalCheckins = 0;
        
        for (Text val : values) {
            // Each value is in the format: "1,reviewCount,sumStars,checkinCount"
            String[] parts = val.toString().split(",");
            if (parts.length < 4) {
                continue;
            }
            totalBusinesses += Integer.parseInt(parts[0]);
            totalReviews += Integer.parseInt(parts[1]);
            sumStars += Double.parseDouble(parts[2]);
            totalCheckins += Integer.parseInt(parts[3]);
        }
        
        double avgStars = (totalReviews == 0) ? 0.0 : sumStars / totalReviews;
        String result = totalBusinesses + "," + totalReviews + "," 
                        + String.format("%.2f", avgStars) + "," + totalCheckins;
        context.write(key, new Text(result));
    }
}
