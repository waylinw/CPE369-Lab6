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
import java.util.ArrayList;
import java.util.HashMap;


public class invertedIndex {
    public static class invertedIndexMapper     // Need to replace the four type labels there with actual Java class names
            extends Mapper< LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> counts = new HashMap<>();
            String text[] =  value.toString().split(",");

            if (text.length == 2) {
                text[1] = text[1].replaceAll("\"", "");
                String words[] = text[1].split(" ");
                for (String word : words) {
                    if (word.equals(" ")) {
                        continue;
                    }
                    String stripped = word.replaceAll("[^\\p{L}\\p{Nd}]+", "");

                    if (counts.containsKey(stripped)) {
                        counts.put(stripped, counts.get(stripped) + 1);
                    }
                    else {
                        counts.put(stripped, 1);
                    }
                }

                for (String word : counts.keySet()) {
                    String output = text[0] + "," + counts.get(word);
                    context.write(new Text(word), new Text(output));
                }
            }

        }
    }

    public static class invertedIndexReducer   // needs to replace the four type labels with actual Java class names
            extends  Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce( Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int documentCount = 0;
            int occurences = 0;
            ArrayList<String> documentIDs = new ArrayList<>();
            for (Text c : values) {
                String[] stats = c.toString().split(",");
                occurences+=Integer.parseInt(stats[1]);
                documentIDs.add(stats[0]);
                documentCount++;
            }

            //formatting output
            String output = occurences + ", " + documentCount + ", [";
            for (String docId : documentIDs) {
                output+= docId + ", ";
            }
            output = output.substring(0, output.length() - 2) + "]";

            context.write(new Text(key), new Text(output));
        }
    }

    public static void main(String args[]) throws Exception{
        // step 1: get a new MapReduce Job object
        Job  job = Job.getInstance();  //  job = new Job() is now deprecated

        // step 2: register the MapReduce class
        job.setJarByClass(invertedIndex.class);

        //  step 3:  Set Input and Output files
        FileInputFormat.addInputPath(job, new Path(args[0])); // put what you need as input file
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // put what you need as output file

        // step 4:  Register mapper and reducer
        job.setMapperClass(invertedIndexMapper.class);
        job.setReducerClass(invertedIndexReducer.class);

        //  step 5: Set up output information
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

        // step 6: Set up other job parameters at will
        job.setJobName("wwang16-Lab6-4");

        // step 8: profit
        System.exit(job.waitForCompletion(true) ? 0:1);
    }


    static class WordCount{
        private int numOccurence;
        private String documentID;
        WordCount(String documentID, int numOccurence) {
            this.numOccurence = numOccurence;
            this.documentID = documentID;
        }

        public int getNumOccurence(){
            return numOccurence;
        }
        public String getDocumentID() {
            return documentID;
        }
    }
}