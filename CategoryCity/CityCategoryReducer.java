import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CityCategoryReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double sum = 0.0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        
        double avg = count == 0 ? 0.0 : sum / count;
        
        // Output format: city, category, average_rating, count
        // We stored city,category in the key
        String[] parts = key.toString().split("\\t");
        String city = parts[0];
        String category = parts[1];
        
        String outLine = city + "\t" + category + "\t" + avg + "\t" + count;
        context.write(NullWritable.get(), new Text(outLine));
    }
}
