import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class UserReviewReducer extends Reducer<Text, Text, Text, Text> {

    public static enum CounterEnum {
        USER_COUNT,
        TOTAL_REVIEWS
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String userInfo = null; 
        int totalReviews = 0;

        for (Text val : values) {
            String line = val.toString();
            String[] parts = line.split("\\|");

            // Check if it's a user record or a review record
            if (parts[0].equals("U")) {
                userInfo = line; 
            } else if (parts[0].equals("R")) {
                totalReviews += 1;
            }
        }

        // If userInfo == null, skip
        if (userInfo == null) {
            return;
        }

        // userInfo => "U|name|complimentHot|complimentMore|complimentProfile"
        String[] userParts = userInfo.split("\\|");
        String userName = userParts[1];
        int complimentHot = Integer.parseInt(userParts[2]);
        int complimentMore = Integer.parseInt(userParts[3]);
        int complimentProfile = Integer.parseInt(userParts[4]);

        // Output format: user_id, plus user info and total reviews
        // e.g. user_id \t userName totalReviews complimentHot complimentMore complimentProfile
        String outValue = String.format("%s\t%d\t%d\t%d\t%d",
                userName, totalReviews, complimentHot, complimentMore, complimentProfile);

        context.write(key, new Text(outValue));

        // Update counters
        context.getCounter(CounterEnum.USER_COUNT).increment(1);
        context.getCounter(CounterEnum.TOTAL_REVIEWS).increment(totalReviews);
    }
}
