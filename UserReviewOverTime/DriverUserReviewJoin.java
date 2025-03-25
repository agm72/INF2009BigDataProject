import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DriverUserReviewJoin {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DriverUserReviewJoin <user.json path> <review.json path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "YelpUserReviewJoin");

        job.setJarByClass(DriverUserReviewJoin.class);

        // We use MultipleInputs to read from user.json and review.json in parallel.
        MultipleInputs.addInputPath(job, new Path(args[0]),
                org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
                UserMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
                ReviewMapper.class);

        // Output from reduce
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // We set the reducer
        job.setReducerClass(UserReviewReducer.class);

        // Our (K, V) from mappers
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Our final (K, V)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
