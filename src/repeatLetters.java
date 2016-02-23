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


public class repeatLetters {

    public static class repeatLettersMapper     // Need to replace the four type labels there with actual Java class names
            extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String word = value.toString().toLowerCase();
            for (int i = 0; i < word.length() - 1; i++) {
                char cur = word.charAt(i);
                char nxt = word.charAt(i + 1);
                if (cur == nxt) {
                    context.write(value, new Text(cur+""));
                    break;
                }
            }

        }
    }

    public static class repeatLettersReducer   // needs to replace the four type labels with actual Java class names
            extends  Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce( Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        // step 1: get a new MapReduce Job object
        Job  job = Job.getInstance();  //  job = new Job() is now deprecated

        // step 2: register the MapReduce class
        job.setJarByClass(repeatLetters.class);

        //  step 3:  Set Input and Output files
        FileInputFormat.addInputPath(job, new Path(args[0])); // put what you need as input file
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // put what you need as output file

        // step 4:  Register mapper and reducer
        job.setMapperClass(repeatLettersMapper.class);
        job.setReducerClass(repeatLettersReducer.class);

        //  step 5: Set up output information
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

        // step 6: Set up other job parameters at will
        job.setJobName("wwang16-Lab6-1");

        // step 8: profit
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}



