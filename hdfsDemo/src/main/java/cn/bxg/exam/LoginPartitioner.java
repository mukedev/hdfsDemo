package cn.bxg.exam;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-29 12:27
 **/
public class LoginPartitioner extends Partitioner<LoginBean, NullWritable> {

    @Override
    public int getPartition(LoginBean loginBean, NullWritable nullWritable, int i) {
        return (loginBean.getUsername() + loginBean.getIp()).hashCode() & Integer.MAX_VALUE % i;
    }
}
