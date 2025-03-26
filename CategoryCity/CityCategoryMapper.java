import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CityCategoryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    private Text outKey = new Text();
    private DoubleWritable outVal = new DoubleWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\t");
        if (parts.length == 3) {
            String city = parts[0];
            String category = parts[1];
            double rating = Double.parseDouble(parts[2]);
            
            String compositeKey = city + "\t" + category;
            outKey.set(compositeKey);
            outVal.set(rating);
            context.write(outKey, outVal);
        }
    }
}
