import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusinessReviewCorrelation {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println(
                "Usage: BusinessReviewCorrelation <businessInput> <reviewInput> <output>"
            );
            System.exit(1);
        }

        String businessInputPath = args[0];
        String reviewInputPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BusinessReviewCorrelation");
        job.setJarByClass(BusinessReviewCorrelation.class);

        // We can use MultipleInputs to specify different mappers for different inputs
        MultipleInputs.addInputPath(job, new Path(businessInputPath),
                TextInputFormat.class, BusinessHoursMapper.class);

        MultipleInputs.addInputPath(job, new Path(reviewInputPath),
                TextInputFormat.class, ReviewMapper.class);

        // We use a single reducer to perform the join and aggregation
        job.setReducerClass(HoursReviewReducer.class);

        // Output types from the reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // job.setNumReduceTasks(1);  // Optionally force 1 reduce task
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
