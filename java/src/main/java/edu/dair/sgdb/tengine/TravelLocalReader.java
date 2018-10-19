package edu.dair.sgdb.tengine;

import edu.dair.sgdb.tengine.travel.Restriction;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.sengine.OrderedRocksDBAPI;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.NIOHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

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
                    //byte[] localVal = this.localStore.get(newKey.getBytes());
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

    public static ArrayList<byte[]> filterVertices_interruptable(
            OrderedRocksDBAPI localstore,
            Set<ByteBuffer> vertices,
            SingleStep currStep, long ts,
            Thread thread) throws InterruptedException {

        ArrayList<byte[]> passedVertices = new ArrayList<>();

        for (ByteBuffer bkey : vertices) {
            if (thread.isInterrupted()){
                throw new InterruptedException();
            }

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
                    //byte[] localVal = this.localStore.get(newKey.getBytes());
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
