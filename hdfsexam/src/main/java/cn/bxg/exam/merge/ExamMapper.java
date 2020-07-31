package cn.bxg.exam.merge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-29 10:45
 **/
public class ExamMapper extends Mapper<LongWritable, Text, Text, LoginBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        LoginBean loginBean = new LoginBean();
        loginBean.setUsername(line[0]);
        loginBean.setIp(line[1]);
        loginBean.setTime(line[2]);
        loginBean.setLoginFlag(Integer.valueOf(line[3]));
        context.write(new Text(line[0]+line[1]),loginBean);
    }
}
