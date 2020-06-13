package cn.bxg.mapreduce.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangYu
 * @date 2020/6/11
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {


    Map<String, String> map = new HashMap<>();

    /**
     * 此方法只会执行一次
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        /**
         * 将缓存数据保存到map
         */
        //1：获取分布式存储列表
        URI[] cacheFiles = context.getCacheFiles();

        //获取指定的分布式缓存文件的文件系统（FileSystem）
        FileSystem fileSystem = FileSystem.newInstance(cacheFiles[0], context.getConfiguration());

        //获取文件的输入流
        FSDataInputStream open = fileSystem.open(new Path(cacheFiles[0]));

        //读取文件的内容并保存到本地map中
        BufferedReader br = new BufferedReader(new InputStreamReader(open));

        String line = "";//br.readLine()每次执行会更改偏移量
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
            map.put(split[0], line);
        }

        //关闭流
        br.close();
        fileSystem.close();


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(",");
        String productId = split[2];
        String productValue = map.get(productId);

        context.write(new Text(productId), new Text(productValue + "," + value));
    }
}
