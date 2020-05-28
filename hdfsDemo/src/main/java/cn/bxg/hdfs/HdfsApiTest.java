package cn.bxg.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author zhangYu
 * @date 2020/5/21
 */
public class HdfsApiTest {

    /**
     * 使用url方式访问数据(了解)
     * 测试：实现文件的copy
     */
    @Test
    public void urlHdfs() throws IOException {

        //第一步：注册hdfs的url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //第二步：获取hdfs文件输入流
        InputStream inputStream = new URL("hdfs://had-node:8020/tmp/dir/a.txt").openStream();
        //第三步：获取输出流
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\hello2.txt"));
        //第四步：实现文件拷贝
        IOUtils.copy(inputStream, outputStream);
        //第五步：关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    /**
     * 使用hdfs文件系统访问数据（掌握）
     */

    /**
     * 获取FileSystem方式1
     *
     * @throws IOException
     */
    @Test
    public void getFileSystem() throws IOException {
        //1.创建configuration对象
        Configuration conf = new Configuration();
        //2.设置文件系统的类型
        conf.set("fs.defaultFS", "hdfs://had-node:8020");
        //3.获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        //4.输出
        System.out.println(fileSystem);
        //5.关闭文件系统
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式2
     *
     * @throws IOException
     */
    @Test
    public void getFileSystem2() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        System.out.println(fileSystem);
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式3
     *
     * @throws IOException
     */
    @Test
    public void getFileSystem3() throws IOException {
        //1.获取configuration对象
        Configuration configuration = new Configuration();
        //2.设置文件系统类型
        configuration.set("fs.defaultFs", "hdfs://had-node:8020");
        //3.获取文件系统实例
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        //4.输出
        System.out.println(fileSystem);
        //5.关流
        fileSystem.close();
    }

    /**
     * 获取FileSystem方式4
     */
    @Test
    public void getFileSystem4() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        System.out.println(fileSystem);
        fileSystem.close();
    }

    /**
     * 遍历api所有文件
     */
    @Test
    public void listMyFiles() throws IOException, URISyntaxException {
        //1.获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.调用listFiles 获取/目录下所有文件信息
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        //3.遍历迭代器
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            //获取文件的绝对路径
            System.out.println(fileStatus.getPath() + "------" + fileStatus.getPath().getName());
            //获取文件block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("block.length:" + blockLocations.length);
        }
        //4.关流
        fileSystem.close();
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdirTest() throws URISyntaxException, IOException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.创建文件夹
        fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        //3.关流
        fileSystem.close();
    }

    /**
     * 创建文件
     */
    @Test
    public void createFile() throws IOException, URISyntaxException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.创建文件
        fileSystem.create(new Path("/aaa/bbb/ccc2/abc.txt"));
        //3.关流
        fileSystem.close();
    }

    /**
     * 文件下载方法1
     */
    @Test
    public void downloadFile() throws URISyntaxException, IOException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.获取输入流
        InputStream inputStream = fileSystem.open(new Path("/aaa/bbb/ccc/abc.txt"));
        //3.获取输出流
        OutputStream outputStream = new FileOutputStream(new File("E:\\tmp/abc.txt"));
        //4.文件拷贝
        IOUtils.copy(inputStream, outputStream);
        //5.关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    /**
     * 文件下载方法2
     */
    @Test
    public void downloadFile2() throws URISyntaxException, IOException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.调用方法下载
        fileSystem.copyToLocalFile(new Path("/aaa/bbb/ccc/abc.txt"), new Path("/E:\\tmp/abc2.txt"));
        //3.关闭流
        fileSystem.close();
    }

    /**
     * 文件上传
     */
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.调用方法实现上传
        fileSystem.copyFromLocalFile(new Path("/E:\\tmp/abc2.txt"), new Path("/aaa/bbb/ccc/a.txt"));
        //3.关闭流
        fileSystem.close();
    }

    /**
     * 小文件合并
     */
    @Test
    public void margeFile() throws URISyntaxException, IOException {
        //1.获取FileSystem
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://had-node:8020"), new Configuration());
        //2.获取输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big.txt"));
        //3.获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //4.获取本地文件夹下的所有详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("E:\\tmp"));
        //5.遍历本地文件
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());

            //6.复制输入流到输出流
            IOUtils.copy(inputStream, outputStream);

            IOUtils.closeQuietly(inputStream);
        }
        //6.关流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }
}
