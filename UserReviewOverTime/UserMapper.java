import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<Object, Text, Text, Text> {

    private Text userIdKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // Each line is a JSON object for a user
        // Example:
        // {
        //   "user_id": "Ha3iJu77CxlrFm-vQRs_8g",
        //   "name": "Sebastien",
        //   ...
        // }

        // Minimal naive parse for user_id and yelping_since.
        // Real code: you might use a JSON library to parse safely.

        String line = value.toString();

        // Extract user_id
        String userId = extractJsonValue(line, "user_id");
        // e.g. "Ha3iJu77CxlrFm-vQRs_8g"

        // Extract yelping_since
        String yelpingSince = extractJsonValue(line, "yelping_since");
        // e.g. "2011-01-01"

        if (userId != null && !userId.isEmpty()) {
            userIdKey.set(userId);

            // We'll store any user-based info we might want. For now, let's keep yelping_since.
            // We put a prefix "U" to indicate this record is from 'User'.
            outValue.set("U\t" + (yelpingSince == null ? "" : yelpingSince));

            context.write(userIdKey, outValue);
        }
    }

    /**
     * Very naive method to extract a field from a line of JSON by key. 
     * Production code should use a real JSON parser.
     */
    private String extractJsonValue(String jsonLine, String field) {
        // Look for `"field":"` or `"field": "`
        // This is a naive approach. Use libraries if possible.
        String search = "\"" + field + "\":";
        int start = jsonLine.indexOf(search);
        if (start == -1) return null;

        // Move index to after "field":
        start += search.length();

        // skip spaces
        while (start < jsonLine.length() && 
               (jsonLine.charAt(start) == ' ' || jsonLine.charAt(start) == '\"')) {
            start++;
        }

        // If the next char is a quote, skip it
        boolean startsWithQuote = (jsonLine.charAt(start) == '\"');
        if (startsWithQuote) start++;

        // read until next quote or comma
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
