import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // We might have 1 record from the business file and 1 record from checkin file
        // But we have to account for edge cases (some businesses have no checkins, etc.)

        Double starValue = null;
        int totalCheckinCount = 0;

        // We might have multiple lines for check-ins, but typically there's one line per business
        // in checkin.json. We'll accumulate them if multiple lines appear (rare).
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String tag = parts[0];  // "B" or "C"
            if (tag.equals("B")) {
                // This is from business.json
                double stars = Double.parseDouble(parts[1]);
                starValue = stars;
            } else if (tag.equals("C")) {
                // This is from checkin.json
                int c = Integer.parseInt(parts[1]);
                totalCheckinCount += c;
            }
        }

        // Only emit if we actually have starValue and some checkin info
        // (Depending on your logic, you might emit starValue=0 if missing, etc.)
        if (starValue != null) {
            // Bin the star rating into a range
            String starRange = StarRangeHelper.getStarRange(starValue);

            // Output key = starRange, value = checkinCount
            context.write(new Text(starRange), new Text(String.valueOf(totalCheckinCount)));
        }
    }
}
