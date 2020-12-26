// The correct mapper and reducer


package Assignment.resultsanddraw;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountMatchDraws {
    public static class MatchDrawsMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text("Records");
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text,IntWritable>.Context context)
            throws IOException, InterruptedException{
            String valueString = value.toString();
            String[] MatchScores = valueString.split(",");
            if (key.get() == 0 &&value.toString().contains("date")){
                return;
            } else if (MatchScores[3].equals(MatchScores[4]))
                context.write(word,one);
        }

    }



public static class CountMatchDrawsReducer
        extends Reducer<Text, IntWritable, NullWritable, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text,IntWritable,NullWritable,IntWritable>.Context context)
            throws IOException, InterruptedException{
            int recordCount = 0;
            for (IntWritable value : values){
                recordCount+= value.get();
            }
            context.write(NullWritable.get(),new IntWritable(recordCount));
        }
}



}
