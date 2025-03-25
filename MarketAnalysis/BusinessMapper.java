import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
// Optionally import a real JSON library, e.g. org.json or Jackson

public class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        // Pseudocode for JSON parsing. For production, use a JSON library.
        // Example approach, searching naive "business_id", "city", "state":

        String businessId = extractField(line, "\"business_id\":");
        if (businessId.isEmpty()) {
            return;  // skip if we can't parse
        }
        String city = extractField(line, "\"city\":");
        String state = extractField(line, "\"state\":");
        if (city.isEmpty()) {
            city = "UnknownCity";
        }
        if (state.isEmpty()) {
            state = "UnknownState";
        }

        outKey.set(businessId);
        outVal.set("B|" + city + "|" + state);
        context.write(outKey, outVal);
    }

    // Minimal naive parsing - in production, replace with robust JSON parse
    private String extractField(String line, String fieldName) {
        int idx = line.indexOf(fieldName);
        if (idx == -1) return "";
        int start = line.indexOf(":", idx) + 1;
        if (start == 0) return "";
        int end = line.indexOf(",", start);
        if (end == -1) {
            // if no comma, look for closing brace
            end = line.indexOf("}", start);
            if (end == -1) {
                end = line.length();
            }
        }
        String raw = line.substring(start, end).replaceAll("[\"{}]", "").trim();
        return raw;
    }
}
