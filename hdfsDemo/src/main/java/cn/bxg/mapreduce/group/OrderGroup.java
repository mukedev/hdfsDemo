package cn.bxg.mapreduce.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 1.继承WritableComparator
 * 2.调用父类的有参构造
 * 3.指定分组规则（重写方法）
 *
 * @author zhangYu
 * @date 2020/6/14
 */
public class OrderGroup extends WritableComparator {

    /**
     * 2.调用父类的有参构造
     */
    public OrderGroup() {
        super(GroupBean.class, true);
    }

    /**
     * 3.指定分组规则（重写方法）
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        GroupBean first = (GroupBean) a;
        GroupBean second = (GroupBean) b;

        //指定分组规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
