package cn.bxg.exam;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description
 * @create 2020-06-29 12:21
 **/
public class LoginBean implements WritableComparable<LoginBean> {

    private String username;
    private String ip;
    private String time;
    private int loginFlag;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getLoginFlag() {
        return loginFlag;
    }

    public void setLoginFlag(int loginFlag) {
        this.loginFlag = loginFlag;
    }

    @Override
    public int compareTo(LoginBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.username);
        dataOutput.writeUTF(this.ip);
        dataOutput.writeUTF(this.time);
        dataOutput.writeInt(this.loginFlag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.username = dataInput.readUTF();
        this.ip = dataInput.readUTF();
        this.time = dataInput.readUTF();
        this.loginFlag = dataInput.readInt();
    }

    @Override
    public String toString() {
        return username + "," + ip + "," + time + "," + loginFlag;
    }
}
