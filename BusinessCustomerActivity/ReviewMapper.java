import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            // Parse as JSON
            JSONObject record = new JSONObject(value.toString());

            String businessId = record.optString("business_id", null);
            if (businessId == null) {
                return;
            }

            // Extract star rating
            double stars = record.optDouble("stars", -1);
            if (stars < 0) {
                // -1 means it wasn't found
                return;
            }

            // Output format: "R <stars>"
            outKey.set(businessId);
            outValue.set("R\t" + stars);
            context.write(outKey, outValue);

        } catch (Exception e) {
            // handle or log error
        }
    }
}
