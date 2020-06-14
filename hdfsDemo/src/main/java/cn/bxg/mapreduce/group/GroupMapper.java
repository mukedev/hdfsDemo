package cn.bxg.mapreduce.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class GroupMapper extends Mapper<LongWritable, Text, GroupBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        GroupBean groupBean = new GroupBean();
        groupBean.setOrderId(line[0]);
        groupBean.setPrice(Double.valueOf(line[2]));

        context.write(groupBean, value);
    }
}
