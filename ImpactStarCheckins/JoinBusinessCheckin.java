import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinBusinessCheckin {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: JoinBusinessCheckin <inputBusiness> <inputCheckin> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JoinBusinessCheckin");

        job.setJarByClass(JoinBusinessCheckin.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // We’ll use a single mapper but specify two input paths
        TextInputFormat.addInputPath(job, new Path(args[0])); // business.json
        TextInputFormat.addInputPath(job, new Path(args[1])); // checkin.json
        job.setInputFormatClass(TextInputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
