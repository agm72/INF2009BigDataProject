import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HoursReviewReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalHours = 0.0;
        boolean hasBusinessInfo = false;

        long reviewCount = 0;
        double sumStars = 0.0;

        // For a given business_id, we get both "B ..." and "R ..." records
        for (Text val : values) {
            String line = val.toString();
            String[] parts = line.split("\t");
            if (parts.length < 2) {
                continue;
            }

            String recordType = parts[0];
            // from business mapper
            if ("B".equals(recordType)) {
                hasBusinessInfo = true;
                totalHours = Double.parseDouble(parts[1]);
            }
            // from review mapper
            else if ("R".equals(recordType)) {
                double stars = Double.parseDouble(parts[1]);
                sumStars += stars;
                reviewCount++;
            }
        }

        // Only write output if we have at least the business info
        // (i.e. the hours)
        if (hasBusinessInfo) {
            double avgStars = (reviewCount > 0) 
                              ? sumStars / reviewCount 
                              : 0.0;
            // Output: business_id \t total_hours \t review_count \t avg_stars
            context.write(
                key,
                new Text(totalHours + "\t" + reviewCount + "\t" + avgStars)
            );
        }
    }
}
