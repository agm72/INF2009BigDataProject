import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2Driver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Job2Driver <job1Output> <job2Output>");
            System.exit(2);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YelpJob2_AggregateCity");
        job.setJarByClass(Job2Driver.class);
        
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
