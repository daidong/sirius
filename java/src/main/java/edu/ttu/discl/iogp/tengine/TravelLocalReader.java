package edu.ttu.discl.iogp.tengine;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.sengine.OrderedRocksDBAPI;
import edu.ttu.discl.iogp.tengine.travel.Restriction;
import edu.ttu.discl.iogp.tengine.travel.SingleStep;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.ArrayPrimitives;
import edu.ttu.discl.iogp.utils.NIOHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

public class TravelLocalReader {

    public static ArrayList<byte[]> filterVertices(
            OrderedRocksDBAPI localstore,
            HashSet<ByteBuffer> vertices,
            SingleStep currStep, long ts) {

        ArrayList<byte[]> passedVertices = new ArrayList<>();

        for (ByteBuffer bkey : vertices) {
            byte[] key = NIOHelper.getActiveArray(bkey);
            Restriction sar = currStep.vertexStaticAttrRestrict;
            Restriction dar = currStep.vertexDynAttrRestrict;
            boolean passSAR = false;
            boolean passDAR = false;

            if (sar != null) {
                byte[] col = sar.key();
                DBKey newKey = new DBKey(key, col, EdgeType.STATIC_ATTR.get());
                KeyValue p = localstore.seekTo(newKey.toKey());
                if (p != null) {
                    byte[] localVal = p.getValue();
                    if (sar.satisfy(localVal)) {
                        passSAR = true;
                    }
                }
            } else {
                passSAR = true;
            }

            if (dar != null) {
                byte[] col = dar.key();
                DBKey newKey = new DBKey(key, col, EdgeType.DYNAMIC_ATTR.get());
                KeyValue p = localstore.seekTo(newKey.toKey());
                if (p != null) {
                    byte[] localVal = p.getValue();
                    //byte[] localVal = this.localstore.get(newKey.getBytes());
                    if (dar.satisfy(localVal)) {
                        passDAR = true;
                    }
                }
            } else {
                passDAR = true;
            }

            if (passSAR && passDAR) {
                passedVertices.add(key);
            }
        }
        return passedVertices;
    }

    public static HashSet<byte[]> scanLocalEdges(OrderedRocksDBAPI localstore,
            ArrayList<byte[]> passedVertices, SingleStep currStep, long ts) {

        HashSet<byte[]> nextVertices = new HashSet<>();

        for (byte[] key : passedVertices) {
            DBKey startKey, endKey;
            startKey = DBKey.MinDBKey(key, EdgeType.OUT.get());
            endKey = DBKey.MaxDBKey(key, EdgeType.OUT.get());

            ArrayList<KeyValue> kvs = localstore.scanKV(startKey.toKey(), endKey.toKey());
            for (KeyValue p : kvs) {
                DBKey dbKey = new DBKey(p.getKey());
                nextVertices.add(dbKey.dst);
            }

        }
        return nextVertices;
    }
}
