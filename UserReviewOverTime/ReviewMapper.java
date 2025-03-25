import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<Object, Text, Text, Text> {

    private Text userIdKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Example line from review.json:
        // {
        //   "review_id": "zdSx_SD6obEhz9VrW9uAWA",
        //   "user_id": "Ha3iJu77CxlrFm-vQRs_8g",
        //   "business_id": "tnhfDv5Il8EaGSXZGiuQGg",
        //   "stars": 4,
        //   "date": "2016-03-09",
        //   "text": "Great place to hang out after work...",
        //   ...
        // }

        String line = value.toString();

        // Extract user_id
        String userId = extractJsonValue(line, "user_id");
        // Extract rating (stars)
        String stars = extractJsonValue(line, "stars");
        // Extract date
        String date = extractJsonValue(line, "date");
        // Extract text
        String reviewText = extractJsonValue(line, "text");

        if (userId != null && !userId.isEmpty()) {
            userIdKey.set(userId);

            // We prefix 'R' to mark "Review".
            // We want year from the date, rating, and review length 
            // for subsequent analysis.
            String year = "";
            if (date != null && date.length() >= 4) {
                year = date.substring(0, 4);
            }
            String rating = (stars == null) ? "0" : stars;
            int length = (reviewText == null) ? 0 : reviewText.length();

            outValue.set("R\t" + year + "\t" + rating + "\t" + length);
            context.write(userIdKey, outValue);
        }
    }

    private String extractJsonValue(String jsonLine, String field) {
        // (Same naive approach as in UserMapper)
        String search = "\"" + field + "\":";
        int start = jsonLine.indexOf(search);
        if (start == -1) return null;

        start += search.length();
        while (start < jsonLine.length() &&
               (jsonLine.charAt(start) == ' ' || jsonLine.charAt(start) == '\"')) {
            start++;
        }

        boolean startsWithQuote = (jsonLine.charAt(start) == '\"');
        if (startsWithQuote) start++;

        StringBuilder sb = new StringBuilder();
        while (start < jsonLine.length()) {
            char c = jsonLine.charAt(start);
            if (c == '\"' || c == ',' || c == '}') {
                break;
            }
            sb.append(c);
            start++;
        }
        return sb.toString().trim();
    }
}
