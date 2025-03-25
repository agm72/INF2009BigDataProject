import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * YelpReviewDistribution:
 *  - Uses two separate mapper classes, one for business.json, one for review.json
 *  - Uses a single reducer to join by business_id and aggregate counts of each star rating
 *  - Outputs: business_id, businessName, count_1star, count_2star, count_3star, count_4star, count_5star
 *
 * Usage:
 *  hadoop jar YelpReviewDistribution.jar com.yelp.hadoop.YelpReviewDistribution \
 *      /path/to/yelp_academic_dataset_business.json \
 *      /path/to/yelp_academic_dataset_review.json \
 *      /output/path
 */
public class YelpReviewDistribution extends Configured implements Tool {

    // ========================================================================
    // Mapper for business.json
    // ========================================================================
    public static class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {

        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse line as JSON
            JsonNode jsonNode = objectMapper.readTree(value.toString());
            // Extract fields
            JsonNode businessIdNode = jsonNode.get("business_id");
            JsonNode nameNode = jsonNode.get("name");

            // Validate
            if (businessIdNode != null && nameNode != null) {
                String businessId = businessIdNode.asText();
                // Replace commas so they don't interfere in final CSV
                String businessName = nameNode.asText().replace(",", " ");

                // We emit:
                // Key = business_id
                // Value = "BUS\t<businessName>"
                context.write(new Text(businessId), new Text("BUS\t" + businessName));
            }
        }
    }

    // ========================================================================
    // Mapper for review.json
    // ========================================================================
    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse line as JSON
            JsonNode jsonNode = objectMapper.readTree(value.toString());
            // Extract fields
            JsonNode businessIdNode = jsonNode.get("business_id");
            JsonNode starsNode = jsonNode.get("stars");

            // Validate
            if (businessIdNode != null && starsNode != null) {
                String businessId = businessIdNode.asText();
                int stars = starsNode.asInt();

                // We emit:
                // Key = business_id
                // Value = "REV\t<stars>"
                context.write(new Text(businessId), new Text("REV\t" + stars));
            }
        }
    }

    // ========================================================================
    // Reducer
    // ========================================================================
    public static class YelpReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Counters for each star rating
            int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0;
            // Default business name if not found
            String businessName = "UNKNOWN";

            for (Text val : values) {
                String line = val.toString();
                String[] parts = line.split("\t", 2); 
                // parts[0] is either "BUS" or "REV"
                // parts[1] is either <businessName> or <stars>

                if (parts[0].equals("BUS")) {
                    // We store the business name
                    businessName = parts[1];
                } else if (parts[0].equals("REV")) {
                    // Increment the relevant star counter
                    int starValue = Integer.parseInt(parts[1]);
                    switch(starValue) {
                        case 1: count1++; break;
                        case 2: count2++; break;
                        case 3: count3++; break;
                        case 4: count4++; break;
                        case 5: count5++; break;
                        default: 
                            // Ignore invalid star rating
                            break;
                    }
                }
            }

            // Format output as CSV-like: businessName, count1, count2, count3, count4, count5
            // The output key is the business_id (key), the output value is the distribution data
            String outValue = businessName + "," 
                            + count1 + "," 
                            + count2 + "," 
                            + count3 + "," 
                            + count4 + "," 
                            + count5;
            context.write(key, new Text(outValue));
        }
    }

    // ========================================================================
    // Driver / Tool
    // ========================================================================
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: YelpReviewDistribution <businessInput> <reviewInput> <output>");
            return -1;
        }

        // Create a new Job
        Job job = Job.getInstance(getConf(), "Yelp Review Distribution");
        job.setJarByClass(YelpReviewDistribution.class);

        // We have two input files, each with their own mapper
        // 1) business.json => BusinessMapper
        // 2) review.json   => ReviewMapper
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, BusinessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, ReviewMapper.class);

        // We use a single reducer that merges the data
        job.setReducerClass(YelpReducer.class);

        // Our output key/value from the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Submit the job and wait for it to complete
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Run this class as a Tool
        int exitCode = ToolRunner.run(new YelpReviewDistribution(), args);
        System.exit(exitCode);
    }
}
