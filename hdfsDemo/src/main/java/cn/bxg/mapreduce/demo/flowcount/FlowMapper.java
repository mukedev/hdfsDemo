package cn.bxg.mapreduce.demo.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/3
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpPackNum(Long.parseLong(line[6]));
        flowBean.setDownPackNum(Long.parseLong(line[7]));
        flowBean.setUpPayLoad(Long.parseLong(line[8]));
        flowBean.setDownPayLoad(Long.parseLong(line[9]));
        context.write(new Text(line[1]), flowBean);
    }
}
