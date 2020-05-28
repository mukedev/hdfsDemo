package cn.hd.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.URL;

import java.io.IOException;
import java.net.URI;
import java.sql.Ref;

/**
 * MapFile读文件
 * @author zhangYu
 * @date 2020/4/5
 */
public class MapFileReader {

    static Configuration configuration = new Configuration();
    private static String url = "hdfs://192.168.144.128:9000";

    public static void main(String[] args) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(url), configuration);
        Path inputPath = new Path("MyMapFile.map");

        MapFile.Reader reader = new MapFile.Reader(fs, inputPath.toString(), configuration);

        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable valueClass = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

        while(reader.next((WritableComparable)keyClass,valueClass)) {
            System.out.println(keyClass);
            System.out.println(valueClass);
        }

        IOUtils.closeStream(reader);
    }

}
