import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;
import java.io.IOException;

public class UserMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Parse the JSON line using org.json
        try {
            JSONObject root = new JSONObject(value.toString());

            // Extract user_id
            if (!root.has("user_id")) {
                // Skip if no user_id
                return;
            }

            String userId = root.getString("user_id");
            outKey.set(userId);

            // Extract relevant user info
            // e.g., name, compliment counts, etc.
            String name = root.optString("name", "unknown");
            int complimentHot = root.optInt("compliment_hot", 0);
            int complimentMore = root.optInt("compliment_more", 0);
            int complimentProfile = root.optInt("compliment_profile", 0);

            // Store them in a string with a "U" tag to distinguish user data
            // Format: U|<name>|<complimentHot>|<complimentMore>|<complimentProfile>
            String combinedUserData = String.format("U|%s|%d|%d|%d",
                    name, complimentHot, complimentMore, complimentProfile);

            outValue.set(combinedUserData);
            context.write(outKey, outValue);

        } catch (Exception e) {
            // If there's a parsing error, you can handle or log it
            // For now, we simply ignore this line
        }
    }
}
