package cn.bxg.mapreduce.demo.flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangYu
 * @date 2020/6/3
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upPackNum = 0;
        long downPackNum = 0;
        long upPayNum = 0;
        long downPayNum = 0;
        for (FlowBean val : values) {
            upPackNum += val.getUpPackNum();
            downPackNum += val.getDownPackNum();
            upPayNum += val.getUpPayLoad();
            downPayNum += val.getDownPayLoad();
        }
        FlowBean flowBean = new FlowBean();
        flowBean.setUpPackNum(upPackNum);
        flowBean.setDownPackNum(downPackNum);
        flowBean.setUpPayLoad(upPayNum);
        flowBean.setDownPayLoad(downPayNum);
        context.write(key, flowBean);
    }
}
