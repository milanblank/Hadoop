package azurehdinsight.mapreduce.cdr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Milan on 10-Sep-17.
 */
public class SumReducer extends Reducer<CdrWritable, IntWritable, Text, IntWritable> {

    private LongWritable user = new LongWritable();
    private LongWritable other = new LongWritable();

    @Override
    protected void reduce(CdrWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalDuration = 0;
        user = key.getUser();
        other = key.getOther();
        String pair = user.toString() + "\t" + other.toString();

        for (IntWritable value: values) {
            totalDuration += value.get();
        }

        context.write(new Text(pair), new IntWritable(totalDuration));
    }
}
