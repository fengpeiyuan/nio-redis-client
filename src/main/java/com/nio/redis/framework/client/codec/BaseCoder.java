package com.nio.redis.framework.client.codec;

/**
 * 
 * ClassName: Base16 <br/> 
 * Function: TODO ADD FUNCTION. <br/> 
 * Reason: TODO ADD REASON. <br/> 
 * date: 2014-5-19 下午4:24:17 <br/> 
 * 
 * @version  
 * @since up JDK 1.5
 */
public class BaseCoder {
    private static final char[] upper  = "0123456789ABCDEF".toCharArray();
    private static final char[] lower  = "0123456789abcdef".toCharArray();
    private static final byte[] decode = new byte[128];

    static {
        for (int i = 0; i < 10; i++) {
            decode['0' + i] = (byte) i;
            decode['A' + i] = (byte) (10 + i);
            decode['a' + i] = (byte) (10 + i);
        }
    }

    /**
     * Encode bytes to base16 chars.
     *
     * @param src   Bytes to encode.
     * @param upper Use upper or lowercase chars.
     *
     * @return Encoded chars.
     */
    public static char[] encode(byte[] src, boolean upper) {
        char[] table = upper ? BaseCoder.upper : BaseCoder.lower;
        char[] dst   = new char[src.length * 2];

        for (int si = 0, di = 0; si < src.length; si++) {
            byte b = src[si];
            dst[di++] = table[(b & 0xf0) >>> 4];
            dst[di++] = table[(b & 0x0f)];
        }

        return dst;
    }

    /**
     * Decode base16 chars to bytes.
     *
     * @param src   Chars to decode.
     *
     * @return Decoded bytes.
     */
    public static byte[] decode(char[] src) {
        byte[] dst = new byte[src.length / 2];

        for (int si = 0, di = 0; di < dst.length; di++) {
            byte high = decode[src[si++] & 0x7f];
            byte low  = decode[src[si++] & 0x7f];
            dst[di] = (byte) ((high << 4) + low);
        }

        return dst;
    }
}
