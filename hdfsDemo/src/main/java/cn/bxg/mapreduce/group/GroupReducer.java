package cn.bxg.mapreduce.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class GroupReducer extends Reducer<GroupBean, Text, Text, NullWritable> {
    @Override
    protected void reduce(GroupBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(val, NullWritable.get());
            System.out.println(val.toString());
            break;
        }
    }
}
