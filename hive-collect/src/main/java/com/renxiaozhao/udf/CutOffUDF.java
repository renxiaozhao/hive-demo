package com.renxiaozhao.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 截断函数.
 * @author https://renxiaozhao.blog.csdn.net/
 *
 */
public class CutOffUDF extends GenericUDF {
    private static final int CUT_LEFT = 0;//截取从左至右标识
    private static final int CUT_RIGHT = -1;//截取从右至左标识
    ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[3];
    
    /**
     * 初始化操作，在函数进行初始化的时候会执行，其他时间不执行.
     */
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 3) {
            throw new UDFArgumentException("参数个数不符合要求，应包含三个参数");
        }
        if (arguments.length == 3) {
            //判断: 参数类型
            if ((((PrimitiveObjectInspector) arguments[0])).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
                throw new UDFArgumentException("第一个参数类型应为String");
            }
            if ((((PrimitiveObjectInspector) arguments[1])).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentException("第二个参数类型应为Int");
            }
            if ((((PrimitiveObjectInspector) arguments[2])).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                throw new UDFArgumentException("第三个参数类型应为Int");
            }
            //参数 <--->参数转换器
            converters[0] = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            converters[1] = ObjectInspectorConverters.getConverter(arguments[1], PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            converters[2] = ObjectInspectorConverters.getConverter(arguments[2], PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    
    /**
     * 进行业务计算逻辑，处理具体的数据.
     */
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        //输入掩码参数
        String input = (String)converters[0].convert(arguments[0].get());
        //0-掩码从左至右；-1-掩码从右至左
        int startIndex =  (Integer)converters[1].convert(arguments[1].get());
        //掩码位数
        int subIndex = (Integer)converters[2].convert(arguments[2].get());
        //掩码处理
        if (startIndex == CUT_LEFT) {
            return input.substring(0, input.length() - subIndex);
        } else if (startIndex == CUT_RIGHT) {
            return input.substring(subIndex);
        } else {
            return input.substring(0,startIndex) + input.substring(subIndex);
        }
    }
    
    /**
     * 进行函数描述结果的显示.
     */
    public String getDisplayString(String[] children) {
        return "cutoffudf('18866866888',0,3) = 66866888";
    }
    
    public static void main(String[] args) {
        String input = "18866866888";
        System.out.println(input.substring(0,4) + input.substring(6));
    
    }
}

