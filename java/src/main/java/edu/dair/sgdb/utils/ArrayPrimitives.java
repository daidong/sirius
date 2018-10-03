package edu.dair.sgdb.utils;

import java.nio.ByteBuffer;

public class ArrayPrimitives {

    //little-endian coding
    public static int btoi(byte[] b, int offset) {
        return (b[offset + 3] & 0xFF) << 24 |
                (b[offset + 2] & 0xFF) << 16 |
                (b[offset + 1] & 0xFF) << 8 |
                (b[offset] & 0xFF);
    }

    public static byte[] itob(int a) {
        byte[] rtn = new byte[4];
        rtn[0] = (byte) (a & 0xFF);
        rtn[1] = (byte) ((a >> 8) & 0xFF);
        rtn[2] = (byte) ((a >> 16) & 0xFF);
        rtn[3] = (byte) ((a >> 24) & 0xFF);
        return rtn;
    }

    public static long btol(byte[] b, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = (value << 8) + (b[offset + i] & 0xff);
        }
        return value;
    }

    public static byte[] ltob(long a) {
        return ByteBuffer.allocate(8).putLong(a).array();
    }

    public static short btos(byte[] b, int offset) {
        return (short) ((b[offset] & 0xFF) | (b[offset + 1] & 0xFF) << 8);
    }

    public static byte[] stob(short a) {
        byte[] rtn = new byte[2];
        rtn[0] = (byte) (a & 0xFF);
        rtn[1] = (byte) ((a >> 8) & 0xFF);
        return rtn;
    }

}
