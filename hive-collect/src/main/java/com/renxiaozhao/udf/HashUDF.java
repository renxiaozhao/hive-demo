package com.renxiaozhao.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.renxiaozhao.udf.util.HmacSHA256Util;

/**
 * 哈希函数.
 * @author lw
 *
 */
public class HashUDF extends GenericUDF {
    ObjectInspectorConverters.Converter[] converters = null;
    
    /**
     * 初始化操作，在函数进行初始化的时候会执行，其他时间不执行.
     */
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        converters = new ObjectInspectorConverters.Converter[arguments.length];
        if (arguments.length == 2) {
            //参数 <--->参数转换器
            converters[0] = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            converters[1] = ObjectInspectorConverters.getConverter(arguments[1], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        } else {
            converters[0] = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }
    
    /**
     * 进行业务计算逻辑，处理具体的数据.
     */
    public String evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length == 2) {
            //输入掩码参数
            String secret = (String)converters[0].convert(arguments[0].get());
            String input = (String)converters[1].convert(arguments[1].get());
            try {
                return HmacSHA256Util.hmacSHA256(secret, input);
            } catch (Exception e) {
                return "函数异常" + e.getMessage();
            }
        } else {
            //输入掩码参数
            String input = (String)converters[0].convert(arguments[0].get());
            try {
                return HmacSHA256Util.hmacSHA256(input);
            } catch (Exception e) {
                return "函数异常" + e.getMessage();
            }
        }
    }
    
    /**
     * 进行函数描述结果的显示.
     */
    public String getDisplayString(String[] children) {
        return "hashudf('18866866888') = 61e0e6df66d7e98a9f6c419841f06f4edffcead115f677a1c35295aa2f46d4ef";
    }
    
}

