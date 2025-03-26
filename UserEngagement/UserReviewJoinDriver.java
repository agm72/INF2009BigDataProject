import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserReviewJoinDriver {
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: UserReviewJoinDriver <user_input> <review_input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UserReviewReduceSideJoin");
        job.setJarByClass(UserReviewJoinDriver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, ReviewMapper.class);

        job.setReducerClass(UserReviewReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        int result = job.waitForCompletion(true) ? 0 : 1;

        // Retrieve counters to compute global average
        long userCount = job.getCounters()
                .findCounter(UserReviewReducer.CounterEnum.USER_COUNT).getValue();
        long totalReviews = job.getCounters()
                .findCounter(UserReviewReducer.CounterEnum.TOTAL_REVIEWS).getValue();

        double avgReviewsPerUser = 0.0;
        if (userCount != 0) {
            avgReviewsPerUser = (double) totalReviews / (double) userCount;
        }

        System.out.println("===========================================");
        System.out.println(" Total Users Processed: " + userCount);
        System.out.println(" Total Reviews: " + totalReviews);
        System.out.println(" Average Reviews/User: " + avgReviewsPerUser);
        System.out.println("===========================================");

        System.exit(result);
    }
}
