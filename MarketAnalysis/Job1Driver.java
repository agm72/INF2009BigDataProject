import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Job1Driver <business> <review> <checkin> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YelpJob1_Merge");
        job.setJarByClass(Job1Driver.class);

        // Business
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BusinessMapper.class);
        // Review
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReviewMapper.class);
        // Checkin
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CheckinMapper.class);

        // Set reducer
        job.setReducerClass(Job1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
