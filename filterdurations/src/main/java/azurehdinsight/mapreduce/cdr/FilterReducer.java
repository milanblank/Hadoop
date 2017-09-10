package azurehdinsight.mapreduce.cdr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value: values) {
            if (value.get() > 1000)
                context.write(key, new IntWritable(value.get()));
        }
    }
}
