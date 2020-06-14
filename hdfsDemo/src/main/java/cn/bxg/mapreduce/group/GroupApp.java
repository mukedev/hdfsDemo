package cn.bxg.mapreduce.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @date 2020/6/14
 */
public class GroupApp {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://had-node:8020/orderGroup/input";
        String outPath = "hdfs://had-node:8020/orderGroup/output";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(outPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        Job job = Job.getInstance(conf, "orderGroup_test");
        job.setJarByClass(GroupApp.class);

        /**
         * mapreduce八大步骤
         */
        //1.设置输入路径和读取类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //2.设置mapper类型和输出kv数据类型
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(GroupBean.class);
        job.setMapOutputValueClass(Text.class);
        //3.分区
        job.setPartitionerClass(GroupPartitioner.class);
        //4.排序
        //5.规约
        //6.分组
        job.setGroupingComparatorClass(OrderGroup.class);
        //7.设置reduce类和输出kv数据类型
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(3);//不设置会有问题
        //8.设置输出路径和输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
