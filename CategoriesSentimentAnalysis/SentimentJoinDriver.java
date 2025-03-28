import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentJoinDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SentimentJoinDriver <review input> <business input> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Yelp Sentiment Analysis with Reduce Side Join");
        job.setJarByClass(SentimentJoinDriver.class);

        // Use MultipleInputs to process two different datasets
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SentimentMapperReview.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SentimentMapperBusiness.class);
        job.setReducerClass(SentimentJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
