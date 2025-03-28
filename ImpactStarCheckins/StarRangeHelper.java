public class StarRangeHelper {
    /**
     * Takes a float star rating and returns a binned range as a string.
     * Example: 3.2 -> "3.0 - 3.5"
     */
    public static String getStarRange(double stars) {
        // For simplicity, round to nearest 0.5
        double rounded = (Math.round(stars * 2.0)) / 2.0; // e.g., 3.2 => 3.0, 3.75 => 3.5
        double lower = rounded;
        double upper = rounded + 0.5;

        // Format "3.0 - 3.5"
        return String.format("%.1f - %.1f", lower, upper);
    }
}
