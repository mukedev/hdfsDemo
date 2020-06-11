package cn.bxg.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-08 10:19
 **/
public class JoinApp {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String inPath = "hdfs://had-node:8020//joinapp/input/";
        String outPath = "hdfs://had-node:8020//joinapp/output/";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //获取job实例并设置主类
        Job job = Job.getInstance(conf, "join_test");
        job.setJarByClass(JoinApp.class);

        /**
         * mapreduce八大步骤
         */
        //1.设置文件输入路径和读取方式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //2.设置mapper类型和kv类型
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //3.分区
        //4.排序
        //5.规约
        //6.分组
        //7.设置reduce类和kv类型
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //8.设置文件输出路径和输出方式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
