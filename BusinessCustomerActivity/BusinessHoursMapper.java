import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

public class BusinessHoursMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            // Parse the line as a JSON object using org.json
            JSONObject record = new JSONObject(value.toString());

            // Extract business_id
            String businessId = record.optString("business_id", null);
            if (businessId == null) {
                return; // skip if no ID
            }

            // 'hours' object is optional
            JSONObject hours = record.optJSONObject("hours");
            if (hours == null) {
                // Some businesses may not have hours info
                return;
            }

            // Calculate the total weekly hours
            double totalHours = 0.0;
            // hours.keys() will give day names (e.g. "Monday", "Tuesday")
            Iterator<String> dayIterator = hours.keys();
            while (dayIterator.hasNext()) {
                String day = dayIterator.next();
                String timeRange = hours.optString(day, "");
                if (timeRange.contains("-")) {
                    // e.g. "10:00-21:00"
                    String[] parts = timeRange.split("-");
                    if (parts.length == 2) {
                        double dailyHours = parseHours(parts[0], parts[1]);
                        totalHours += dailyHours;
                    }
                }
            }

            // Output with prefix "B"
            outKey.set(businessId);
            outValue.set("B\t" + totalHours);
            context.write(outKey, outValue);

        } catch (Exception e) {
            // For example, org.json.JSONException or parse issues
            // Log or ignore to continue
        }
    }

    /**
     * Helper method that takes a start time and end time (HH:MM) 
     * and returns the difference in hours (e.g., 10:00 - 21:00 -> 11.0).
     */
    private double parseHours(String start, String end) {
        try {
            String[] startParts = start.split(":");
            String[] endParts = end.split(":");

            int startHour = Integer.parseInt(startParts[0]);
            int startMin = Integer.parseInt(startParts[1]);
            int endHour = Integer.parseInt(endParts[0]);
            int endMin = Integer.parseInt(endParts[1]);

            double startDecimal = startHour + (startMin / 60.0);
            double endDecimal = endHour + (endMin / 60.0);

            // Basic assumption: times do not cross midnight
            return endDecimal - startDecimal;
        } catch (Exception ex) {
            return 0.0;
        }
    }
}
