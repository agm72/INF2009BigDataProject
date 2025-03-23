import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class SentimentMapperReview extends Mapper<Object, Text, Text, Text> {
    // A small set of positive and negative words (for demo purposes)
    private static final String[] positiveWords = {"good", "great", "excellent", "amazing", "love"};
    private static final String[] negativeWords = {"bad", "terrible", "awful", "worst", "hate"};

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject jsonObj = new JSONObject(value.toString());
            String businessId = jsonObj.optString("business_id", "unknown");
            String reviewText = jsonObj.optString("text", "").toLowerCase();
            int posCount = 0, negCount = 0;
            for (String word : reviewText.split("\\W+")) {
                for (String pos : positiveWords) {
                    if (word.equals(pos)) posCount++;
                }
                for (String neg : negativeWords) {
                    if (word.equals(neg)) negCount++;
                }
            }
            int sentimentScore = posCount - negCount;
            // Tag the record as a review record ("R")
            context.write(new Text(businessId), new Text("R|" + sentimentScore));
        } catch (Exception e) {
            System.err.println("Error processing review JSON: " + e.getMessage());
        }
    }
}
