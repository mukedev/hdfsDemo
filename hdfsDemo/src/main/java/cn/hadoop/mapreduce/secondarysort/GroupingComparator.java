package cn.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zhangYu
 * @date 2020/4/6
 */
public class GroupingComparator implements RawComparator<IntPair> {

    @Override
    public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
        return WritableComparator.compareBytes(bytes,i,Integer.SIZE/8,bytes1,i2,Integer.SIZE/8);
    }

    @Override
    public int compare(IntPair o1, IntPair o2) {
        int first1 = o1.getFirst();
        int first2 = o2.getFirst();
        return first1 - first2;
    }
}
