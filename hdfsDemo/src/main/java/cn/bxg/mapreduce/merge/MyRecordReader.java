package cn.bxg.mapreduce.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/13
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private Configuration conf = null;
    private FileSplit fileSplit = null;
    private boolean processed = false;
    BytesWritable bytesWritable = new BytesWritable();
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;

    /**
     * 进行初始化工作
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        conf = taskAttemptContext.getConfiguration();
        //获取文件切片
        fileSplit = (FileSplit) inputSplit;
    }

    /**
     * 此方法用于生车k1-v1
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            //1:获取源文件的字节输入流
            //1.1 获取源文件的文件系统（FileSystem）
            Path path = new Path("hdfs://had-node:8020");
            fileSystem = path.getFileSystem(conf);
            //1.2 通过FileSystem获取文件的输入流
            System.out.println(fileSplit.getPath());
            inputStream = fileSystem.open(fileSplit.getPath());
            //2:读取源文件的数据到普通字节数组
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            //读取到bytes
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());
            //3:将普通字节数据封装到BytesWritable
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }
        return false;
    }

    /**
     * 返回k1
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 获取进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }

}
