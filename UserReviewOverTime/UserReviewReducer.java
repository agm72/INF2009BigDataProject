import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewReducer extends Reducer<Text, Text, Text, Text> {

    // We only store the user-level info if needed; for instance,
    // yelping_since. We might not strictly need it for the final analysis,
    // but let's see how we might incorporate it.

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // key = user_id
        // Each value is either "U<TAB>yelping_since" or "R<TAB>year<TAB>stars<TAB>reviewLen"

        String yelpingSince = "";
        List<String> reviews = new ArrayList<>();

        for (Text val : values) {
            String line = val.toString();
            String[] tokens = line.split("\\t");

            if (tokens[0].equals("U")) {
                // user record
                if (tokens.length > 1) {
                    yelpingSince = tokens[1];
                }
            } else if (tokens[0].equals("R")) {
                // review record
                reviews.add(line); 
                // we'll parse further below
            }
        }

        // We have all the user info & reviews for this user.
        // Next: we can group the reviews by year and compute average rating, average length, etc.

        // year -> [ (stars, length), (stars, length), ... ]
        // We'll do a quick aggregator.
        java.util.Map<String, YearStats> yearStatsMap = new java.util.HashMap<>();

        for (String r : reviews) {
            // r = "R\tyear\tstars\treviewLen"
            String[] tokens = r.split("\\t");
            if (tokens.length == 4) {
                String year = tokens[1];
                String starsStr = tokens[2];
                String lengthStr = tokens[3];

                double starsVal = Double.parseDouble(starsStr);
                int lenVal = Integer.parseInt(lengthStr);

                YearStats ys = yearStatsMap.getOrDefault(year, new YearStats());
                ys.count++;
                ys.sumStars += starsVal;
                ys.sumLength += lenVal;
                yearStatsMap.put(year, ys);
            }
        }

        // For each year, compute average rating, average length
        // We'll emit (user_id, year + other stats)
        for (String year : yearStatsMap.keySet()) {
            YearStats ys = yearStatsMap.get(year);

            double avgStars = ys.sumStars / ys.count;
            double avgLen = ys.sumLength / (double) ys.count;

            // For the final output, we can store yelpingSince if desired
            // Key: user_id
            // Value: year, yelpingSince, totalReviewsInYear, avgStars, avgLength
            String outputVal = year + "\t" 
                    + yelpingSince + "\t"
                    + ys.count + "\t"
                    + String.format("%.2f", avgStars) + "\t"
                    + String.format("%.2f", avgLen);

            context.write(key, new Text(outputVal));
        }
    }

    static class YearStats {
        int count;
        double sumStars;
        int sumLength;
    }
}
