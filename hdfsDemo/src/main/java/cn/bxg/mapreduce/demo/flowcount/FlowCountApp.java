package cn.bxg.mapreduce.demo.flowcount;

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
 * @date 2020/6/3
 */
public class FlowCountApp {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String inPath = "hdfs://had-node:8020/flowCount/input";
        String outPath = "hdfs://had-node:8020/flowCount/output";

        //判断是否存在输出路径，存在则删除
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //获取job实例
        Job job = Job.getInstance(conf, "flow_count");
        job.setJarByClass(FlowCountApp.class);

        /**
         * mapreduce八大步骤
         */
        //配置输入文件路径和读取方式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //配置mapper的主类和输出数据类型
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //分区
        //排序
        //规约
        //分组
        //配置reduce的主类和输出数据类型
        job.setReducerClass(FlowReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //配置结果文件输出路径和输出方式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));

        //退出程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
