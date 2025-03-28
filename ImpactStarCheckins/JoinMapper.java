import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Parse the line as JSON
        String line = value.toString();
        try {
            JSONObject json = new JSONObject(line);

            // Check if it's business.json or checkin.json by presence of certain fields
            if (json.has("stars")) {
                // It's from business.json
                String businessId = json.getString("business_id");
                double stars = json.getDouble("stars");

                // output key = business_id
                outKey.set(businessId);
                // output value = "B" + delimiter + stars
                outValue.set("B\t" + stars);
                context.write(outKey, outValue);

            } else if (json.has("date")) {
                // It's from checkin.json
                String businessId = json.getString("business_id");
                String dateString = json.getString("date");
                // dateString is "2016-04-26 19:49:16, 2016-08-30 18:36:57, ..."
                // Just split on commas to count check-ins
                String[] timestamps = dateString.split(",");
                int checkinCount = timestamps.length;

                outKey.set(businessId);
                // output value = "C" + delimiter + checkinCount
                outValue.set("C\t" + checkinCount);
                context.write(outKey, outValue);
            }
        } catch (Exception e) {
            // If any parsing error occurs, we can skip or handle it
        }
    }
}
