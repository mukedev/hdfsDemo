package cn.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class MyReducer extends Reducer<IntPair, IntWritable, Text, IntWritable> {

    private static final Text SEPARATOR = new Text("------------");
    private final Text first = new Text();

    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(SEPARATOR,null);
        first.set(Integer.toString(key.getFirst()));
        for (IntWritable value : values) {
            context.write(first, value);
        }
    }
}
