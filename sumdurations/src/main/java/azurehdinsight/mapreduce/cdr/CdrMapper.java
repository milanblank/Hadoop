package azurehdinsight.mapreduce.cdr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Milan on 10-Sep-17.
 */
public class CdrMapper extends Mapper<LongWritable, Text, CdrWritable, IntWritable> {

    private CdrWritable cdr = new CdrWritable();
    private LongWritable user = new LongWritable();
    private LongWritable other = new LongWritable();
    private IntWritable duration = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split(",");

            if (fields.length > 0) {
                user.set(Long.parseLong(fields[0].trim()));
                other.set(Long.parseLong(fields[1].trim()));
                duration.set(Integer.parseInt(fields[3].trim()));

                cdr = new CdrWritable(user, other);

                context.write(cdr, duration);
            }
        } catch (NumberFormatException e){
            System.out.println("Method parseLong() or parseInt() has thrown an exception");
            e.printStackTrace();
        }

    }
}
