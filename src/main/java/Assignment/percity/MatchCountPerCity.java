package Assignment.percity;

import org.apache.commons.lang.StringEscapeUtils;
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

public class MatchCountPerCity {


        public static class WordCountMapper extends
                Mapper<Object, Text, Text, IntWritable> {

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {

                // Parse the input string into a nice map
                Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
                        .toString());

                // Grab the "Text" field, since that is what we are counting over
                String city = parsed.get("city");

                // .get will return null if the key is not there
                if (city == null) {
                    // skip this record
                    return;
                }

                // Unescape the HTML because the SO data is escaped.
                city = StringEscapeUtils.unescapeHtml(city.toLowerCase());

                // Remove some annoying punctuation
                city = city.replaceAll("'", ""); // remove single quotes (e.g., can't)
                city = city.replaceAll("[^a-zA-Z]", " "); // replace the rest with a
                // space

                // Tokenize the string, then send the tokens away
                StringTokenizer itr = new StringTokenizer(city);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, one);
                }
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
            job.setJarByClass(MatchCountPerCity.class);
            job.setMapperClass(WordCountMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    
}
