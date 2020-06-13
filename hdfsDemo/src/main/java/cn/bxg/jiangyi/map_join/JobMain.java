package cn.bxg.jiangyi.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inPath = "hdfs://had-node:8020/mapjoin/input";
        String outPath = "hdfs://had-node:8020/mapjoin/output";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), conf);
        if (fileSystem.exists(new Path(outPath))) {
            fileSystem.delete(new Path(outPath), true);
        }

        //1:获取job对象
        Job job = Job.getInstance(super.getConf(), "map_join0_job");
        job.setJarByClass(JobMain.class);
        //2:设置job对象(将小表放在分布式缓存中)
        //将小表放在分布式缓存中
//     DistributedCache.addCacheFile(new URI("hdfs://node01:8020/cache_file/product.txt"), super.getConf());
        job.addCacheFile(new URI("hdfs://had-node:8020/mapjoin/cache/product.txt"));

        //第一步:设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inPath));
        //第二步:设置Mapper类和数据类型
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //第八步:设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outPath));


        //3:等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //加了这个配置使用FileSystem.get（new URI(),conf）获取缓存不会报FileSyste.closed,但是不建议改成true.默认是false
        //可以使用newInstance()获取文件系统
        //configuration.set("fs.hdfs.impl.disable.cache","true");
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
