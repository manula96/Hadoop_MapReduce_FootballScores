package Assignment.resultsanddraw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import Assignment.MRDPUtils;


import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class CountMatchResultsandDraw {
    public static class ScoreMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text word2 = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse the input string into a nice map
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                    .toString());

            // Grab the "Text" field, since that is what we are counting over
            String home_score = parsed.get("home_score");
            String away_score = parsed.get("away_score");

            // .get will return null if the key is not there
            if (home_score == null || away_score  == null) {
                // skip this record
                return;
            }

//            // Unescape the HTML because the SO data is escaped.
//            txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

/*            // Remove some annoying punctuation
            txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
            txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a
            // space*/

            int counter = 0;
            // Tokenize the string, then send the tokens away
            StringTokenizer itr = new StringTokenizer(home_score);
            StringTokenizer itr2 = new StringTokenizer(away_score);
            while (itr.hasMoreTokens() || itr2.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
                word2.set(itr2.nextToken());
                context.write(word2, one);

                if (home_score == away_score){
                    counter = counter+1;

                }

            }

            System.out.println(counter);

        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CountMatchResultsandDraw.class);
        job.setMapperClass(CountMatchResultsandDraw.ScoreMapper.class);
        job.setCombinerClass(CountMatchResultsandDraw.IntSumReducer.class);
        job.setReducerClass(CountMatchResultsandDraw.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
