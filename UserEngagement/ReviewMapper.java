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
            JSONObject root = new JSONObject(value.toString());

            // Extract user_id
            if (!root.has("user_id")) {
                return;
            }
            String userId = root.getString("user_id");
            outKey.set(userId);

            // For each review, we simply emit "R|1" so we can count them in the reducer
            outValue.set("R|1");
            context.write(outKey, outValue);

        } catch (Exception e) {
            // Handle or log JSON parsing exceptions as appropriate
        }
    }
}
