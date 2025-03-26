import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class YelpJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // We need to parse the JSON to figure out if it's a business or a review.
        // We'll handle it by checking for certain fields.
        String line = value.toString();
        
        // Use a generic JSON object read
        try {
            // Convert line to a Map
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> record = 
                JSON_MAPPER.readValue(line, java.util.Map.class);
            
            if (record.containsKey("business_id") && record.containsKey("name")) {
                // Likely from business.json
                String businessId = (String) record.get("business_id");
                String city = (String) record.get("city");
                // Some data might be missing or null, handle gracefully
                if (city == null) city = "Unknown";
                
                // Extract categories as a comma-separated string
                // "categories" might be an array or a single string. We handle carefully
                Object categoriesObj = record.get("categories");
                String categoryStr = "";
                if (categoriesObj instanceof java.util.List) {
                    categoryStr = String.join(";", 
                        (java.util.List<String>) categoriesObj);
                } else if (categoriesObj instanceof String) {
                    categoryStr = (String) categoriesObj;
                }
                
                // Tag the output so the Reducer knows this is a "business record"
                // Format: "B|city|categories"
                outKey.set(businessId);
                String out = "B|" + city + "|" + categoryStr;
                outValue.set(out);
                context.write(outKey, outValue);
                
            } else if (record.containsKey("business_id") && record.containsKey("stars") 
                    && record.containsKey("review_id")) {
                // Likely from review.json
                String businessId = (String) record.get("business_id");
                // "stars" might be an integer or double
                double stars = Double.parseDouble(record.get("stars").toString());
                
                outKey.set(businessId);
                // Tag: "R|stars"
                outValue.set("R|" + stars);
                context.write(outKey, outValue);
            }
            
        } catch (Exception e) {
            // If parsing fails, we simply skip the record or log an error
            System.err.println("Parsing error: " + e.getMessage());
        }
    }
}
