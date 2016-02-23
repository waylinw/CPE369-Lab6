/**
 * Waylin Wang CPE 369 - 3
 * Lab 6
 */
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import java.io.IOException;
import java.math.BigDecimal;


public class scores {

    public static class scoresMapper     // Need to replace the four type labels there with actual Java class names
            extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parsed = value.toString().split(",");
            if (parsed.length == 4) {
                String id = parsed[0].replaceAll("\\s+","");
                int quantity = (int) Double.parseDouble(parsed[1]);
                double price = Double.parseDouble(parsed[2]);
                double shipping = Double.parseDouble(parsed[3]);
                double total = 0;
                if (quantity < 100) {
                    total = quantity * price + shipping;
                }
                else {
                    total = 100 * price + (shipping/quantity) * 100;
                    total +=  (quantity - 100) * price + (shipping/quantity) * (quantity - 100);
                }

                //Rounding price to 2 decimal places
                BigDecimal big = new BigDecimal(total);
                big = big.setScale(2, BigDecimal.ROUND_HALF_UP);

                String val = quantity + "," + String.format("%.2f",big.doubleValue());

                context.write(new Text(id), new Text(val));
            }
        }
    }

    public static class scoresReducer   // needs to replace the four type labels with actual Java class names
            extends  Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce( Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalQuantity = 0;
            double totalProfit = 0;
            double profit  = 0;
            for(Text temp : values) {
                String[] parsed = temp.toString().split(",");
                totalQuantity += (int) Double.parseDouble(parsed[0]);
                totalProfit += Double.parseDouble(parsed[1]);
            }

            String outVal = totalQuantity + ", " + String.format("%.2f",totalProfit);

            context.write(key, new Text(outVal));

        }
    }

    public static void main(String[] args) throws Exception {
        // step 1: get a new MapReduce Job object
        Job  job = Job.getInstance();  //  job = new Job() is now deprecated

        // step 2: register the MapReduce class
        job.setJarByClass(scores.class);

        //  step 3:  Set Input and Output files
        FileInputFormat.addInputPath(job, new Path(args[0])); // put what you need as input file
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // put what you need as output file

        // step 4:  Register mapper and reducer
        job.setMapperClass(scoresMapper.class);
        job.setReducerClass(scoresReducer.class);

        //  step 5: Set up output information
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

        // step 6: Set up other job parameters at will
        job.setJobName("wwang16-Lab6-3");

        // step 8: profit
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
