package com.fanx.kafka;

public class ByteUtils {
    public static byte[] longToBytes(long res){
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            /*
            * 一个字节对应一个数组元素,先存高位后存低位
            * & 0xff:这是为了右移操作后,数字的大小超过255的情况下让取值等于最后的8位数字
            * 得到的结果就是将64位的long拆分成:byte[i]=将64位的long型数字从左到右的顺序取走第i个8位数字
            */
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((res >> offset) & 0xff);
        }
        return buffer;
    }

    public static long bytesToLong(byte[] bytes) {
        long values = 0;
        for (int i = 0; i < 8; i++) {
            /*
            * 设置一个变量用来存放bytes数组中的值
            * 循环bytes数组,每次循环将这个变量向左移动一个字节空位,然后填补这个字节
             */
            values <<=8;
            values |= (bytes[i] & 0xff);
        }
        return values;
    }


    public static void main(String[] args) {
        final long test = 248934093478L;
        System.out.println(bytesToLong(longToBytes(test)));
    }
}
