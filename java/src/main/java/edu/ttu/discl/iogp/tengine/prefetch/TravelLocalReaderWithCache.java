package edu.ttu.discl.iogp.tengine.prefetch;

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

public class TravelLocalReaderWithCache {

    public static ArrayList<byte[]> filterVertices(OrderedRocksDBAPI localstore,
                                                   HashSet<ByteBuffer> vertices, SingleStep currStep, long ts) {

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
                DBKey newKey = new DBKey(key, col, EdgeType.STATIC_ATTR.get(), ts);
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
                DBKey newKey = new DBKey(key, col, EdgeType.DYNAMIC_ATTR.get(), ts);
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

    public static HashSet<byte[]> scanLocalEdges(OrderedRocksDBAPI localstore, PreLoadMemoryPool pool,
            ArrayList<byte[]> passedVertices, SingleStep currStep, long ts) {
        HashSet<byte[]> nextVertices = new HashSet<>();
        ArrayList<byte[]> copyOfVertices = new ArrayList<>(passedVertices);
        
        for (byte[] key : copyOfVertices) {
            ByteBuffer bkey = ByteBuffer.wrap(key);
            HashSet<ByteBuffer> localEdges = pool.getEdges(bkey);
            if (localEdges != null){

                for (ByteBuffer dst : localEdges){
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

                // we scan local edges, Currently and also By Default, we only keep the newest version for each data.
                for (int edge : edgeTypes) {
                    DBKey startKey, endKey;
                    if (start == null && end == null) { //time is reversed
                        startKey = DBKey.MinDBKey(key, edge, ts);
                        endKey = DBKey.MaxDBKey(key, edge, 0L);
                    } else {                            //time is reversed
                        startKey = new DBKey(key, start, edge, ts);
                        endKey = new DBKey(key, end, edge, 0L);
                    }

                    HashSet<ByteBuffer> contains = new HashSet<ByteBuffer>();
                    //GLogger.debug("startKey: %s -> endKey: %s", startKey.toString(), endKey.toString());
                    ArrayList<KeyValue> kvs = localstore.scanKV(startKey.toKey(), endKey.toKey());
                    for (KeyValue p : kvs) {
                        DBKey dbKey = new DBKey(p.getKey());
                        long currTs = dbKey.ts;
                        ByteBuffer dst = ByteBuffer.wrap(dbKey.dst);

                        if (currTs <= ts) {
                            if (!contains.contains(dst)) {
                                contains.add(dst);
                                nextVertices.add(dbKey.dst);
                            }
                        }
                    }
                }
            }
        }
        return nextVertices;
    }
}
