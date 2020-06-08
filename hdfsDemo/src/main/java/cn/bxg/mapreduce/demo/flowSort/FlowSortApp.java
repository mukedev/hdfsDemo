package cn.bxg.mapreduce.demo.flowSort;

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
 * @create 2020-06-08 11:31
 **/
public class FlowSortApp {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String inPath = "hdfs://had-node:8020/flowCount/output";
        String outPath = "hdfs://had-node:8020/flowCount/sort_out";

        //判断是否存在输出路径，存在则删除
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //获取job实例
        Job job = Job.getInstance(conf, "flow_count_sort");
        job.setJarByClass(FlowSortApp.class);

        /**
         * mapreduce八大步骤
         */
        //配置输入文件路径和读取方式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //配置mapper的主类和输出数据类型
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //分区
        //排序
        //规约
        //分组
        //配置reduce的主类和输出数据类型
        job.setReducerClass(FlowReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //配置结果文件输出路径和输出方式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        //退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
