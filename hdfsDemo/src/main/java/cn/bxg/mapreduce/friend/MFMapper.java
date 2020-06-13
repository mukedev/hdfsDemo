package cn.bxg.mapreduce.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/13
 */
public class MFMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String k = split[0];
        String[] val = split[1].split(",");
        for (String v : val) {
            context.write(new Text(v), new Text(k));
        }
    }
}
