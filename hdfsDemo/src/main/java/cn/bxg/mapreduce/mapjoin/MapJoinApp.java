package cn.bxg.mapreduce.mapjoin;

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
 * @date 2020/6/11
 */
public class MapJoinApp {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String inPath = "hdfs://had-node:8020/mapjoin/input";
        String outPath = "hdfs://had-node:8020/mapjoin/output";
        String cachePath = "hdfs://had-node:8020/mapjoin/cache/product.txt";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(inPath), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath),true);
        }

        ///加了这个配置使用FileSystem.get（new URI(),conf）获取缓存不会报FileSyste.closed,但是不建议改成true.默认是false
        //可以使用newInstance()获取文件系统
        //configuration.set("fs.hdfs.impl.disable.cache","true");

        //获取job实例并设置主类
        Job job = Job.getInstance(conf, "mapJoin_test");
        job.setJarByClass(MapJoinApp.class);

        //将hdfs的商品表放入缓存
        job.addCacheFile(new URI(cachePath));

        /**
         * 八大步骤
         */
        //1:设置文件输入路径和输入格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(inPath));
        //2:设置mapper主类和k-v数据类型
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //3:分区
        //4:排序
        //5:规约
        //6:分组
        //7:设置reduce主类和k-v数据类型
        //8:设置文件的输出路径和输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(outPath));

        //退出
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
