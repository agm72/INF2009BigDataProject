import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> sentimentScores = new ArrayList<>();
        String categories = "unknown";
        for (Text val : values) {
            String record = val.toString();
            String[] parts = record.split("\\|", 2);
            if (parts[0].equals("B")) {
                categories = parts[1];
            } else if (parts[0].equals("R")) {
                sentimentScores.add(Integer.parseInt(parts[1]));
            }
        }
        // Aggregate sentiment score for the business
        int totalSentiment = 0;
        for (int score : sentimentScores) {
            totalSentiment += score;
        }
        // Output: business_id, categories, totalSentiment, count of reviews (optional)
        context.write(key, new Text("Categories: " + categories + " | Sentiment: " + totalSentiment));
    }
}
