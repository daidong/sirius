package edu.dair.sgdb.tengine.prefetch;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.sengine.OrderedRocksDBAPI;
import edu.dair.sgdb.tengine.sync.SyncTravelEngine;
import edu.dair.sgdb.tengine.travel.Restriction;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by daidong on 12/14/15.
 */
public class SyncTravelPreFetcher {

    AbstractSrv instance;
    SyncTravelEngine engine;
    ArrayList<SingleStep> plans;
    long travelId;
    int currStepId;
    long ts;
    HashSet<ByteBuffer> vertices;

    class Fetcher implements Runnable {

        @Override
        public void run() {
            GLogger.debug("S PL %d %d %d", instance.getLocalIdx(), 0, System.nanoTime());

            try {
                SingleStep preload = plans.get(currStepId + 1);
                OrderedRocksDBAPI localstore = instance.localStore;

                int totalStep = plans.size();
                int prefetchStep = currStepId + 1;

                engine.pool.addVertices(vertices);

                while (prefetchStep < totalStep) {
                    /**
                     * Preload Edges
                     */
                    HashSet<ByteBuffer> nexts = new HashSet<ByteBuffer>();

                    for (ByteBuffer bkey : vertices) {

                        if (engine.is_travel_step_started(travelId, (currStepId + 1))) {
                            return;
                        }

                        if (engine.pool.has(bkey)) {
                            continue;
                        }

                        byte[] key = NIOHelper.getActiveArray(bkey);
                        Restriction edgeType = preload.typeRestrict;
                        Restriction edgeKey = preload.edgeKeyRestrict;
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
                            startKey = DBKey.MinDBKey(key, edge);
                            endKey = DBKey.MaxDBKey(key, edge);

                            ArrayList<KeyValue> kvs = localstore.scanKV(startKey.toKey(), endKey.toKey());
                            for (KeyValue p : kvs) {
                                DBKey dbKey = new DBKey(p.getKey());
                                ByteBuffer dst = ByteBuffer.wrap(dbKey.dst);
                                engine.pool.addEdge(bkey, dst);
                                nexts.add(dst);
                            }
                        }
                    }
                    vertices = nexts;
                    prefetchStep += 1;
                }

                /**
                 * Try to prefetch more aggressively?
                 */
            } finally {
                //GLogger.warn("Cached: %d", engine.pool.getCachedItemNumber());
                GLogger.info("R PL %d %d %d", instance.getLocalIdx(), 0, System.nanoTime());
            }
        }
    }

    public SyncTravelPreFetcher(AbstractSrv srv, SyncTravelEngine e, HashSet<ByteBuffer> byteBuffers,
                                ArrayList<SingleStep> plans,
                                long travelId, int stepId, long ts) {
        this.instance = srv;
        this.engine = e;
        this.plans = plans;
        this.travelId = travelId;
        this.currStepId = stepId;
        this.ts = ts;
        this.vertices = byteBuffers;
    }

    public void start() {
        instance.prefetchPool.execute(new Fetcher());
    }
}
