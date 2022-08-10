-- 掩码函数
create function rxz_mask as "com.renxiaozhao.udf.MaskUDF";
-- 截断函数
create function rxz_cutoff as "com.renxiaozhao.udf.CutOffUDF";
-- 哈希函数
create function rxz_hash as "com.renxiaozhao.udf.HashUDF";
-- AES解密函数
create function rxz_aesd as "com.renxiaozhao.udf.AESDecryptUDF";
-- AES加密函数
create function rxz_aese as "com.renxiaozhao.udf.AESEncryptUDF";