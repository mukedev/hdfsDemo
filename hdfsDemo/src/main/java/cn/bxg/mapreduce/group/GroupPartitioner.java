package cn.bxg.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class GroupPartitioner extends Partitioner<GroupBean, Text> {
    @Override
    public int getPartition(GroupBean groupBean, Text text, int i) {
        return (groupBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
