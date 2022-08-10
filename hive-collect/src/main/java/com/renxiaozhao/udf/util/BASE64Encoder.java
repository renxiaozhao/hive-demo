package com.renxiaozhao.udf.util;

/**
 * BASE64Encoder.
 * @author lenovo
 *
 */
public class BASE64Encoder {
    private static char[] codec_table = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
        '5', '6', '7', '8', '9', '+', '/' };

    public static String encode(byte[] a) {
        int totalBits = a.length * 8;
        int nn = totalBits % 6;
        int curPos = 0;
        StringBuffer toReturn = new StringBuffer();
        while (curPos < totalBits) {
            int bytePos = curPos / 8;
            switch (curPos % 8) {
                case 0:
                    toReturn.append(codec_table[((a[bytePos] & 0xFC) >> 2)]);
                    break;
                case 2:
                    toReturn.append(codec_table[(a[bytePos] & 0x3F)]);
                    break;
                case 4:
                    if (bytePos == a.length - 1) {
                        toReturn.append(codec_table[((a[bytePos] & 0xF) << 2 & 0x3F)]);
                    } else {
                        int pos = ((a[bytePos] & 0xF) << 2 | (a[(bytePos + 1)] & 0xC0) >> 6) & 0x3F;
                        toReturn.append(codec_table[pos]);
                    }
                    break;
                case 6:
                    if (bytePos == a.length - 1) {
                        toReturn.append(codec_table[((a[bytePos] & 0x3) << 4 & 0x3F)]);
                    } else {
                        int pos = ((a[bytePos] & 0x3) << 4 | (a[(bytePos + 1)] & 0xF0) >> 4) & 0x3F;
                        toReturn.append(codec_table[pos]);
                    }
                    break;
                case 1:
                case 3:
                case 5:
                default:
            }
            curPos += 6;
        }
        if (nn == 2) {
            toReturn.append("==");
        } else if (nn == 4) {
            toReturn.append("=");
        }
        return toReturn.toString();
    }

    public static byte[] encodeToByte(byte[] src) {
        int end = src.length;
        int len = 4 * ((end + 2) / 3);
        byte[] dst = new byte[len];
        int sp = 0;
        int slen = end / 3 * 3;
        int sl = slen;
        int dp = 0;
        while (sp < sl) {
            int sl0 = Math.min(sp + slen, sl);
            int sp0 = sp;
            for (int dp0 = dp; sp0 < sl0; ) {
                int bits = (src[(sp0++)] & 0xFF) << 16 | (src[(sp0++)] & 0xFF) << 8 | src[(sp0++)] & 0xFF;
                dst[(dp0++)] = (byte) codec_table[(bits >>> 18 & 0x3F)];
                dst[(dp0++)] = (byte) codec_table[(bits >>> 12 & 0x3F)];
                dst[(dp0++)] = (byte) codec_table[(bits >>> 6 & 0x3F)];
                dst[(dp0++)] = (byte) codec_table[(bits & 0x3F)];
            }
            int dlen = (sl0 - sp) / 3 * 4;
            dp += dlen;
            sp = sl0;
        }
        if (sp < end) {
            int b0 = src[(sp++)] & 0xFF;
            dst[(dp++)] = (byte) codec_table[(b0 >> 2)];
            if (sp == end) {
                dst[(dp++)] = (byte) codec_table[(b0 << 4 & 0x3F)];
                dst[(dp++)] = 61;
                dst[(dp++)] = 61;
            } else {
                int b1 = src[(sp++)] & 0xFF;
                dst[(dp++)] = (byte) codec_table[(b0 << 4 & 0x3F | b1 >> 4)];
                dst[(dp++)] = (byte) codec_table[(b1 << 2 & 0x3F)];
                dst[(dp++)] = 61;
            }
        }
        if (dp != len) {
            byte[] copy = new byte[dp];
            System.arraycopy(dst, 0, copy, 0, Math.min(dst.length, dp));
            return copy;
        }
        return dst;
    }

}