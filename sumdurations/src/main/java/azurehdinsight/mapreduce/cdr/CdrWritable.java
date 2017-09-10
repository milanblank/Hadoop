package azurehdinsight.mapreduce.cdr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CdrWritable implements WritableComparable<CdrWritable>
{
    private LongWritable user, other;

    public CdrWritable() {
        this.user = new LongWritable();
        this.other = new LongWritable();
    }

    public CdrWritable(LongWritable user, LongWritable other) {
        this.user = user;
        this.other = other;
    }

    public LongWritable getUser() {
        return user;
    }

    public LongWritable getOther() {
        return other;
    }

    public void readFields(DataInput in) throws IOException {
        user.readFields(in);
        other.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        user.write(out);
        other.write(out);
    }

    public int compareTo(CdrWritable o) {
        if(user.compareTo(o.user) == 0)
            return other.compareTo(o.other);
        else
            return user.compareTo(o.user);
    }

    @Override
    public int hashCode() {
        return user.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if( o instanceof CdrWritable){
            CdrWritable other = (CdrWritable) o;
            return user.equals(other.user);
        }
        return false;
    }

    @Override
    public String toString() {
        return user + ", " + other;
    }
}
