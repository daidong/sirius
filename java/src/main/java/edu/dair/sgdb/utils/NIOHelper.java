/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.dair.sgdb.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author daidong
 */
public class NIOHelper {

    public static byte[] getActiveArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        if (buffer.hasArray()) {
            byte[] array = buffer.array();
            System.arraycopy(array, buffer.arrayOffset() + buffer.position(), ret, 0, ret.length);
        } else {
            buffer.slice().get(ret);
        }
        return ret;
    }


    public static byte[] ByteBufferToArray(ByteBuffer payload) {
        byte[] array = payload.array();
        int start = payload.arrayOffset() + payload.position();
        int end = payload.arrayOffset() + payload.limit();
        byte[] load = Arrays.copyOfRange(array, start, end);
        return load;
    }
}
