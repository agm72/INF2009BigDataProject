import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String businessId = extractField(line, "\"business_id\":");
        if (businessId.isEmpty()) {
            return;
        }
        String starStr = extractField(line, "\"stars\":");
        if (starStr.isEmpty()) {
            return;
        }

        outKey.set(businessId);
        outVal.set("R|" + starStr);
        context.write(outKey, outVal);
    }

    private String extractField(String line, String fieldName) {
        int idx = line.indexOf(fieldName);
        if (idx == -1) return "";
        int start = line.indexOf(":", idx) + 1;
        if (start == 0) return "";
        int end = line.indexOf(",", start);
        if (end == -1) {
            end = line.indexOf("}", start);
            if (end == -1) {
                end = line.length();
            }
        }
        String raw = line.substring(start, end).replaceAll("[\"{}]", "").trim();
        return raw;
    }
}
