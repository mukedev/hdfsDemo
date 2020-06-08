package cn.bxg.mapreduce.demo.flowpartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/3
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlowBean flowBean = new FlowBean();

        String[] line = value.toString().split("\t");
        flowBean.setUpPackNum(Long.parseLong(line[1]));
        flowBean.setDownPackNum(Long.parseLong(line[2]));
        flowBean.setUpPayLoad(Long.parseLong(line[3]));
        flowBean.setDownPayLoad(Long.parseLong(line[4]));

        context.write(flowBean, new Text(line[0]));
    }
}
