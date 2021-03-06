package cn.hd;

import org.apache.hadoop.io.Writable;

import java.io.ByteArrayOutputStream;
import java.io.*;

/**
 * 序列化工具类
 * @author zhangYu
 * @date 2020/4/4
 */
public class HadoopSerializationUtil {

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(out);
        writable.write(dataout);
        dataout.close();
        return out.toByteArray();
    }

    public static void deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datain = new DataInputStream(in);
        writable.readFields(datain);
        datain.close();

    }
}
