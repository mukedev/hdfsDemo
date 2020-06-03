package cn.bxg.mapreduce.sort;

import cn.hadoop.mapreduce.sort.MyReducer;
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
 * @date 2020/5/31
 */
public class SortApp {

    public static void main(String ages[]) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://had-node:8020/sort/input";
        String outPath = "hdfs://had-node:8020/sort/output";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
    }

        //获取job实例并设置主类
        Job job = Job.getInstance(conf, "sort_text");
        job.setJarByClass(SortApp.class);

        /**
         * mapReduce八大步骤
         */
        //1.配置文件的路径和读取方式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //2.配置mapper的主类和key-value数据类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //3.分区
        //4.排序
        //5.规约
        //6.分组
        //7.配置reduce的主类和数据类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);
        //8.配置数据文件输出方式和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
