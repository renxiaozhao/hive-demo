package com.renxiaozhao.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * 掩码函数.
 * @author https://renxiaozhao.blog.csdn.net/
 *
 */
@SuppressWarnings("deprecation")
public class MaskUDF extends UDF {
    private static final int MASK_LEFT = 0;//掩码从左至右标识
    private static final int MASK_RIGHT = -1;//掩码从右至左标识
    
    /**
     * 掩码处理.
     * @param input 掩码输入参数
     * @param startIndex 掩码从左至右标识:0-掩码从左至右；-1-掩码从右至左；其他则按位截取，比如2-从第三位开始截取
     * @param subIndex 掩码位数
     * @return
     * @throws HiveException
     */
    public String evaluate(String input,Integer startIndex,Integer subIndex) throws HiveException {
        //掩码处理
        String append = "";
        if (startIndex == MASK_LEFT || startIndex == MASK_RIGHT) {
            for (int i = 0;i < subIndex;i++) {
                append += "*";
            }
        } else {
            for (int i = 0;i < (subIndex - startIndex);i++) {
                append += "*";
            }
        }
        if (startIndex == MASK_LEFT) {
            return append + input.substring(subIndex);
        } else if (startIndex == MASK_RIGHT) {
            return input.substring(0, input.length() - subIndex) + append;
        } else {
            return input.substring(0,startIndex) + append + input.substring(subIndex);
        }
    }
    
    //测试
    public static void main(String[] args) {
        String input = "18866866888";
        String append = "";
        for (int i = 0;i < (6 - 4);i++) {
            append += "*";
        }
        System.out.println(input.substring(0,4) + append + input.substring(6));
    }
}

