package cn.bxg.mapreduce.sort2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description sort
 * @create 2020-06-01 09:22
 **/
public class SortApp {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://had-node:8020/sort/input";
        String outPath = "hdfs://had-node:8020/sort/output";

        //获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort_test2");
        job.setJarByClass(SortApp.class);

        //判断文件系统是否存在路径
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        /**
         * 八大步骤
         */
        //1.配置文件的路径和读取文件格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //2.配置mapper的主类和输出数据类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //3.分区；4.排序；5.规约；6.分组
        //7.配置reduce的主类和数据数据类型
        job.setReducerClass(MyReduce.class);
        job.setOutputValueClass(SortBean.class);
        job.setOutputKeyClass(NullWritable.class);
        //8.配置输出路径和输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
