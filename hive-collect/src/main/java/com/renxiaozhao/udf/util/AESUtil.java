package com.renxiaozhao.udf.util;

import java.lang.reflect.Field;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * AESUtil.
 *
 */
public class AESUtil {
    public static final String ALGORITHM = "AES/ECB/PKCS5Padding";
    private static String k = "DYeK6cNZuG155Aw+LcHqNeJ779aNzwiVfP4ILFhpmZg=";
    public static String UTF8 = "UTF-8";
    public static String GBK = "GBK";

    public static String encrypt(String content) throws Exception {
        return encrypt(k, content, UTF8);
    }

    public static String encryptGBK(String content) throws Exception {
        return encrypt(k, content, GBK);
    }

    public static String encrypt(String content, String charset) throws Exception {
        return encrypt(k, content, charset);
    }

    public static String encrypt(String key, String content, String charset) throws Exception {
        if ((content == null) || ("".equals(content))) {
            return content;
        }
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        SecretKeySpec keySpec = new SecretKeySpec(BASE64Decoder.decode(key), "AES");
        cipher.init(1, keySpec);
        byte[] bytes = cipher.doFinal(content.getBytes(charset));
        return BASE64Encoder.encode(bytes);
    }

    public static String decryptJS(String key, String content) throws Exception {
        return decrypt(key, content, "UTF-8");
    }

    public static String decrypt(String content) throws Exception {
        return decrypt(k, content, UTF8);
    }

    public static String decryptGBK(String content) throws Exception {
        return decrypt(k, content, GBK);
    }

    public static String decrypt(String content, String charset) throws Exception {
        return decrypt(k, content, charset);
    }

    public static String decrypt(String key, String content, String charset) throws Exception {
        if ((content == null) || ("".equals(content))) {
            return content;
        }
        byte[] bytes = BASE64Decoder.decode(content);
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        SecretKeySpec keySpec = new SecretKeySpec(BASE64Decoder.decode(key), "AES");
        cipher.init(2, keySpec);
        byte[] decoded = cipher.doFinal(bytes);
        return new String(decoded, charset);
    }

    public static void removeCryptographyRestrictions() {
        try {
            Class.forName("javax.crypto.JceSecurity");
        } catch (ClassNotFoundException e1) {
            System.out.println("去除jdk的128位的限制失败");
            return;
        }
        try {
            Class.forName("javax.crypto.CryptoPermissions");
        } catch (ClassNotFoundException e1) {
            System.out.println("去除jdk的128位的限制失败");
            return;
        }
        try {
            Class.forName("javax.crypto.CryptoAllPermission");
        } catch (ClassNotFoundException e1) {
            System.out.println("去除jdk的128位的限制失败");
            return;
        }

        try {
            Class jceSecurity = Class.forName("javax.crypto.JceSecurity");
            Class cryptoPermissions = Class.forName("javax.crypto.CryptoPermissions");
            Class cryptoAllPermission = Class.forName("javax.crypto.CryptoAllPermission");

            Field isRestrictedField = jceSecurity.getDeclaredField("isRestricted");
            isRestrictedField.setAccessible(true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(isRestrictedField, isRestrictedField.getModifiers() & 0xFFFFFFEF);
            isRestrictedField.set(null, Boolean.valueOf(false));

            Field defaultPolicyField = jceSecurity.getDeclaredField("defaultPolicy");
            defaultPolicyField.setAccessible(true);
            PermissionCollection defaultPolicy = (PermissionCollection) defaultPolicyField.get(null);

            Field perms = cryptoPermissions.getDeclaredField("perms");
            perms.setAccessible(true);
            ((Map) perms.get(defaultPolicy)).clear();

            Field instance = cryptoAllPermission.getDeclaredField("INSTANCE");
            instance.setAccessible(true);
            defaultPolicy.add((Permission) instance.get(null));
        } catch (Exception e) {
            System.out.println("去除jdk的128位的限制失败");
            e.printStackTrace();
        }
    }

    static {
        removeCryptographyRestrictions();
    }

}