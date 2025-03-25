import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String city = "UnknownCity";
        String state = "UnknownState";

        int reviewCount = 0;
        // Use double for sumStars to handle half-stars or decimals (e.g. 4.5)
        double sumStars = 0.0;
        int checkinCount = 0;

        for (Text val : values) {
            String record = val.toString();
            String[] parts = record.split("\\|");
            // parts[0] = B or R or C

            if (parts[0].equals("B")) {
                // B|city|state
                if (parts.length >= 3) {
                    city = parts[1];
                    state = parts[2];
                }
            } else if (parts[0].equals("R")) {
                // R|stars => might be "5.0", "4.5", etc.
                if (parts.length >= 2) {
                    try {
                        double starVal = Double.parseDouble(parts[1]);
                        reviewCount++;
                        sumStars += starVal;  // Keep the decimal
                    } catch (NumberFormatException e) {
                        // If we can't parse, skip or handle error
                        continue;
                    }
                }
            } else if (parts[0].equals("C")) {
                // C|checkinCount
                if (parts.length >= 2) {
                    try {
                        checkinCount += Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        // skip or handle error
                    }
                }
            }
        }

        // If there's no business info, skip
        if (city.equals("UnknownCity")) {
            return;
        }

        // Build partial aggregates for THIS business
        // We do "1" for total businesses, then reviewCount, sumStars, checkinCount
        // We'll sum them in the second job
        String outString = String.format("1,%d,%.1f,%d", reviewCount, sumStars, checkinCount);

        // We can store city+state in the key, or just city
        context.write(new Text(city + "," + state), new Text(outString));
    }
}
