package edu.dair.sgdb.tengine.prefetch;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.sengine.OrderedRocksDBAPI;
import edu.dair.sgdb.tengine.travel.Restriction;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.NIOHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

public class TravelLocalReaderWithCache {

    public static ArrayList<byte[]> filterVertices(OrderedRocksDBAPI localstore,
                                                   HashSet<ByteBuffer> vertices,
                                                   SingleStep currStep, long ts) {

        ArrayList<byte[]> passedVertices = new ArrayList<>();
        ArrayList<ByteBuffer> copyOfVertices = new ArrayList<>(vertices);

        for (ByteBuffer bkey : copyOfVertices) {
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

    public static HashSet<byte[]> scanLocalEdges(OrderedRocksDBAPI localstore, PreLoadMemoryPool pool,
                                                 ArrayList<byte[]> passedVertices, SingleStep currStep, long ts) {
        HashSet<byte[]> nextVertices = new HashSet<>();
        ArrayList<byte[]> copyOfVertices = new ArrayList<>(passedVertices);

        for (byte[] key : copyOfVertices) {
            ByteBuffer bkey = ByteBuffer.wrap(key);
            HashSet<ByteBuffer> localEdges = pool.getEdges(bkey);
            if (localEdges != null) {

                for (ByteBuffer dst : localEdges) {
                    nextVertices.add(NIOHelper.getActiveArray(dst));
                }

            } else {

                Restriction edgeType = currStep.typeRestrict;
                Restriction edgeKey = currStep.edgeKeyRestrict;
                ArrayList<Integer> edgeTypes = new ArrayList<Integer>();

                if (edgeType != null) {
                    for (byte[] v : edgeType.values()) {
                        edgeTypes.add(ArrayPrimitives.btoi(v, 0));
                    }
                }

                byte[] start = null;
                byte[] end = null;
                // edgeKey can only be RANGE
                if (edgeKey != null) {
                    start = ((Restriction.Range) edgeKey).starter();
                    end = ((Restriction.Range) edgeKey).end();
                }

                // we scan local edges,
                // Currently and also By Default, we only keep the newest version for each data.
                for (int edge : edgeTypes) {
                    DBKey startKey, endKey;
                    startKey = DBKey.MinDBKey(key, edge);
                    endKey = DBKey.MaxDBKey(key, edge);

                    ArrayList<KeyValue> kvs = localstore.scanKV(startKey.toKey(), endKey.toKey());
                    for (KeyValue p : kvs) {
                        DBKey dbKey = new DBKey(p.getKey());
                        nextVertices.add(dbKey.dst);
                    }
                }
            }
        }
        return nextVertices;
    }
}
