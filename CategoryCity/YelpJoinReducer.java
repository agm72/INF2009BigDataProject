import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class YelpJoinReducer extends Reducer<Text, Text, NullWritable, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        String city = null;
        String categories = null;
        List<Double> ratings = new ArrayList<>();
        
        for (Text val : values) {
            String[] parts = val.toString().split("\\|", 2); 
            // We used B|city|categories or R|stars
            String tag = parts[0];
            
            if (tag.equals("B")) {
                // This is the business record
                String[] bParts = val.toString().split("\\|", 3);
                // bParts[0] = "B"
                // bParts[1] = city
                // bParts[2] = categories
                city = bParts.length > 1 ? bParts[1] : "Unknown";
                categories = bParts.length > 2 ? bParts[2] : "";
                
            } else if (tag.equals("R")) {
                // This is the review record
                String[] rParts = val.toString().split("\\|", 2);
                // rParts[1] = stars
                double stars = Double.parseDouble(rParts[1]);
                ratings.add(stars);
            }
        }
        
        // If we have valid business info, output for each review
        // Format: city, category, rating
        if (city != null && !ratings.isEmpty()) {
            // categories might be multiple, separated by semicolons
            // We can choose to output one line per category per rating
            String[] catArray = categories.split(";");
            
            for (double r : ratings) {
                for (String cat : catArray) {
                    if (!cat.trim().isEmpty()) {
                        // Output a single line with city, cat, rating
                        String outLine = city + "\t" + cat.trim() + "\t" + r;
                        context.write(NullWritable.get(), new Text(outLine));
                    }
                }
            }
        }
    }
}
