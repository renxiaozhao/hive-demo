package com.renxiaozhao.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.renxiaozhao.udf.util.AESUtil;
import com.renxiaozhao.udf.util.AESUtil2;

/**
 * AES加密.
 * @author lenovo
 *
 */
public class AESEncryptUDF extends UDF {

    /**
     * 默认key、字符集UTF-8.
     * @param content
     * @return
     * @throws Exception
     */
    public static String evaluate(String content) throws Exception {
        return AESUtil2.encrypt(content);
    }

    /**
     * 默认key、可指定字符集，传空默认UTF-8.
     * @param content
     * @param charset
     * @return
     * @throws Exception
     */
    public static String evaluate(String content, String charset) throws Exception {
        if (charset == null || "".equals(charset)) {
            charset = AESUtil.UTF8;
        }
        return AESUtil2.encrypt(content, charset);
    }

    /**
     * 可指定key、字符集，传空默认UTF-8.
     * @param secretKey
     * @param content
     * @param charset
     * @return
     * @throws Exception
     */
    public String evaluate(String secretKey, String content, String charset) throws Exception {
        if (secretKey == null || "".equals(secretKey)) {
            throw new IllegalArgumentException("secretKey can not be empty...");
        }
        if (charset == null || "".equals(charset)) {
            charset = AESUtil.UTF8;
        }
        return AESUtil2.encrypt(secretKey, content, charset);
    }

    public static void main(String[] args) {
        try {
            System.out.println(new AESEncryptUDF().evaluate("Copote@2022#"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}