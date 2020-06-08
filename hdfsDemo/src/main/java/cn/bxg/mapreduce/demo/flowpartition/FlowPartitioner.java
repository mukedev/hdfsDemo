package cn.bxg.mapreduce.demo.flowpartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-08 13:13
 **/
public class FlowPartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        String phone = text.toString();
        if (phone.startsWith("139")) {
            return 1;
        } else if (phone.startsWith("135")) {
            return 2;
        } else if (phone.startsWith("137")) {
            return 3;
        }
        return 0;
    }
}
