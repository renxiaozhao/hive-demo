package com.renxiaozhao.udf.util;
/**
 * AESUtil2.
 *
 */
public class AESUtil2 {
    public static String encrypt(String content) {
        try {
            return AESUtil.encrypt(content);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String encryptGBK(String content) {
        try {
            return AESUtil.encryptGBK(content);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String encrypt(String content, String charset) {
        try {
            return AESUtil.encrypt(content, charset);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String encrypt(String key, String content, String charset) {
        try {
            return AESUtil.encrypt(key, content, charset);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String decryptJS(String key, String content) {
        try {
            return AESUtil.decryptJS(key, content);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String decrypt(String content) {
        try {
            return AESUtil.decrypt(content);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String decryptGBK(String content) {
        try {
            return AESUtil.decryptGBK(content);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String decrypt(String content, String charset) {
        try {
            return AESUtil.decrypt(content, charset);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

    public static String decrypt(String key, String content, String charset) {
        try {
            return AESUtil.decrypt(key, content, charset);
        } catch (Exception e) {
            content += "转换异常" + e.getMessage();
        }
        return content;
    }

}