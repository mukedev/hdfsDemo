package cn.hd;

import java.io.IOException;

/**
 * 序列化测试
 * @author zhangYu
 * @date 2020/4/4
 */
public class Demo02 {

    public static void main(String[] args) throws IOException {

        //测试序列化
        Person person = new Person("zhangsan", 27, "man");
        byte[] serialize = HadoopSerializationUtil.serialize(person);

        //测试反序列化
        Person p = new Person();
        HadoopSerializationUtil.deserialize(p,serialize);
        System.out.println(p);
    }
}
