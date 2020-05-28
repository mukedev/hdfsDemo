package cn.hadoop.mapreduce.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.awt.*;

/**
 * @author zhangYu
 * @date 2020/4/5
 */
public class MyPartitioner extends Partitioner<LongWritable, Text> {


    @Override
    public int getPartition(LongWritable key, Text value, int numPartitions) {
        //偶数放到第二个分区进行计算
        if (key.get()%2 == 0){
            //将输入到reduce中的key设置为1
            key.set(1);
            return 1;
        } else {//奇数放在第一个分区进行计算
            //将输入到reduce中的key设置为0
            key.set(0);
            return 0;
        }
    }
}
