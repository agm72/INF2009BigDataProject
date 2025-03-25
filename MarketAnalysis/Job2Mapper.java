import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outVal = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        // Expecting: "city,state<TAB>1,reviewCount,sumStars,checkinCount"
        String[] parts = line.split("\\t");
        if (parts.length != 2) {
            return;
        }
        outKey.set(parts[0]);
        outVal.set(parts[1]);
        context.write(outKey, outVal);
    }
}
