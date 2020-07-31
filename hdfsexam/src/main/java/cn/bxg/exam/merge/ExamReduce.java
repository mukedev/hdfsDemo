package cn.bxg.exam.merge;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-29 10:46
 **/
public class ExamReduce extends Reducer<Text, LoginBean, Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<LoginBean> values, Context context) throws IOException, InterruptedException {
        String first ="" ;
        String second ="";
        for (LoginBean loginBean: values) {
            if (loginBean.getLoginFlag() == 1){
                first = loginBean.toString();
            }else {
                second = loginBean.getTime();
            }
        }
        context.write(new Text(first + "," + second),NullWritable.get());
    }
}
