import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class SentimentMapperBusiness extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject jsonObj = new JSONObject(value.toString());
            String businessId = jsonObj.optString("business_id", "unknown");
            // For simplicity, assume categories is a comma-separated string
            String categories = jsonObj.optString("categories", "unknown");
            // Tag the record as a business record ("B")
            context.write(new Text(businessId), new Text("B|" + categories));
        } catch (Exception e) {
            System.err.println("Error processing business JSON: " + e.getMessage());
        }
    }
}
