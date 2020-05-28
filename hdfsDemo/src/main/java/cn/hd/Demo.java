package cn.hd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;


/**
 * @author zhangYu
 * @date 2020/4/4
 */
public class Demo {

    public static final String HDFS_PATH = "hdfs://192.168.144.128:9000";

    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp()");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration);
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
        System.out.println("HDFSApp.tearDown()");
    }

    /***
     *
     * 使用java Api 操作hdfs
     *
     ***/

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsap/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsap/test/a.txt"));
        output.write("hello World".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsap/test/a.txt");
        Path newPath = new Path("/hdfsap/test/b.txt");
        System.out.println(fileSystem.rename(oldPath,newPath));
    }

    /**
     * 上传本地文件到hadoop
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path local = new Path("E:/hello.txt");
        Path path = new Path("/hdfsap/test");
        fileSystem.copyFromLocalFile(local,path);
    }

    /**
     * 查看某个目录下的所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsap/test"));
        for (FileStatus fileStatus : fileStatuses) {
            boolean isDir = fileStatus.isDirectory();
            String permission = fileStatus.getPermission().toString();//权限
            short replication = fileStatus.getReplication();//副本系数
            long len = fileStatus.getLen();//长度
            String path = fileStatus.getPath().toString();//路径
            System.out.println(isDir?"文件夹":"文件" + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }

    }

    /**
     * 查看文件块信息
     * @throws Exception
     */
    @Test
    public void getFileBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsap/test/b.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String host : block.getHosts()){
                System.out.println(host);
            }
        }
    }

}
