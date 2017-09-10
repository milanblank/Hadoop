package azurehdinsight.mapreduce.cdr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private IntWritable duration = new IntWritable();
    private Text pair = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split("\t");

            if (fields.length > 0) {
                pair.set(fields[0].trim() + "\t" + fields[1].trim());
                duration.set(Integer.parseInt(fields[2].trim()));

                context.write(pair, duration);
            }
        } catch (NumberFormatException e){
            System.out.println("Method parseLong() has thrown exception");
            e.printStackTrace();
        }
    }
}
