package edu.dair.sgdb.sengine;

import edu.dair.sgdb.utils.ArrayPrimitives;

import java.util.Arrays;

public class DBKey {

    /**
     * |src|type|dst|
     */
    public byte[] src;
    public int type;
    public byte[] dst;

    public final static int MAX_SUBKEY_LEN = 1024;
    public static final byte[] MAX_SHORT_BYTE = new byte[MAX_SUBKEY_LEN];
    public static final byte[] MIN_SHORT_BYTE = {(byte) 0x00};

    static {
        for (int i = 0; i < MAX_SUBKEY_LEN; i++) {
            MAX_SHORT_BYTE[i] = (byte) 0xFF;
        }
    }

    public static DBKey MaxDBKey() {
        return new DBKey(MAX_SHORT_BYTE, MAX_SHORT_BYTE, Integer.MAX_VALUE);
    }

    public static DBKey MaxDBKey(byte[] src) {
        DBKey max = new DBKey(src, MAX_SHORT_BYTE, Integer.MAX_VALUE);
        return max;
    }

    public static DBKey MaxDBKey(byte[] src, int type) {
        DBKey max = new DBKey(src, MAX_SHORT_BYTE, type);
        return max;
    }

    public static DBKey MinDBKey() {
        return new DBKey(MIN_SHORT_BYTE, MIN_SHORT_BYTE, 0);
    }

    public static DBKey MinDBKey(byte[] src) {
        DBKey min = new DBKey(src, MIN_SHORT_BYTE, 0);
        return min;
    }

    public static DBKey MinDBKey(byte[] src, int type) {
        DBKey min = new DBKey(src, MIN_SHORT_BYTE, type);
        return min;
    }


    public DBKey(byte[] s, byte[] d, int t) {
        this.src = s;
        this.dst = d;
        this.type = t;
    }

    public DBKey(byte[] data) {
        int offset = 0;
        short srcLen = ArrayPrimitives.btos(data, offset);
        offset += 2;
        byte[] src = new byte[srcLen];
        System.arraycopy(data, offset, src, 0, srcLen);
        offset += srcLen;

        int edgeType = ArrayPrimitives.btoi(data, offset);
        offset += 4;

        short dstLen = ArrayPrimitives.btos(data, offset);
        offset += 2;
        byte[] dst = new byte[dstLen];
        System.arraycopy(data, offset, dst, 0, dstLen);
        offset += dstLen;

        DBKey k = new DBKey(src, dst, edgeType);
        this.src = src;
        this.dst = dst;
        this.type = edgeType;
    }

    public int size() {
        return (src.length + 2 + dst.length + 2 + 4);
    }

    public byte[] toKey() {
        int capacity = src.length + 2 + dst.length + 2 + 4;
        byte[] key = new byte[capacity];

        short srcLen = (short) src.length;
        byte[] srcLenBytes = ArrayPrimitives.stob(srcLen);
        int offset = 0;
        System.arraycopy(srcLenBytes, 0, key, offset, srcLenBytes.length);
        offset += srcLenBytes.length;
        //GLogger.debug("total key size:%d; src size: %d; offset: %d", key.length, src.length, offset);
        System.arraycopy(src, 0, key, offset, srcLen);
        offset += srcLen;

        byte[] typeBytes = ArrayPrimitives.itob(type);
        System.arraycopy(typeBytes, 0, key, offset, typeBytes.length);
        offset += typeBytes.length;

        short dstLen = (short) dst.length;
        byte[] dstLenBytes = ArrayPrimitives.stob(dstLen);
        System.arraycopy(dstLenBytes, 0, key, offset, dstLenBytes.length);
        offset += dstLenBytes.length;
        System.arraycopy(dst, 0, key, offset, dstLen);
        offset += dstLen;

        return key;
    }

    public String toString() {
        return Arrays.toString(src) + ":" + type + ":" + Arrays.toString(dst);
    }
}
