package cn.bxg.exam;

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
public class ExamReduce extends Reducer<NullWritable, LoginBean, Text,NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<LoginBean> values, Context context) throws IOException, InterruptedException {
        String first ="" ;
        String second ="";
        for (LoginBean loginBean: values) {
            if (loginBean.getLoginFlag() == 1){
                first = loginBean.toString();
            }else {
                second = loginBean.toString();
            }
        }
        context.write(new Text(first + "," + second),NullWritable.get());
    }
}
