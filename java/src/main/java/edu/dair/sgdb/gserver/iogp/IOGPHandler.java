package edu.dair.sgdb.gserver.iogp;

import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.tengine.abfs.abfs;
import edu.dair.sgdb.tengine.bfs.bfs;
import edu.dair.sgdb.tengine.sync.SyncTravelEngine;
import edu.dair.sgdb.gserver.BaseHandler;
import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class IOGPHandler extends BaseHandler {

    public IOGPSrv inst = null;

    public IOGPHandler(IOGPSrv s) {
        this.inst = s;
        this.bfs_engine = new bfs(s);
        this.abfs_engine = new abfs(s);
        this.syncEngine = new SyncTravelEngine(s);
    }

    @Override
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws TException {
        //GLogger.info("[%d]-[START]-[%s]", inst.getLocalIdx(), "insert");

        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);

        DBKey newKey = new DBKey(bsrc, bdst, type);

        inst.edgecounters.putIfAbsent(src, new Counters());
        Counters c = inst.edgecounters.get(src);
        c.actual_exist = 1;

        synchronized (c) {
            /**
             * Initial Status. loc is empty and but current server is src's hash location
             */
            if (!inst.loc.containsKey(src) &&
                    inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                inst.loc.put(src, inst.getLocalIdx());
                inst.split.put(src, 0);
            }

            if (!inst.loc.containsKey(dst) &&
                    inst.getHashLocation(bdst, inst.serverNum) == inst.getLocalIdx()) {
                inst.loc.put(dst, inst.getLocalIdx());
                inst.split.put(dst, 0);
            }
            /*
             *  Vertex Insertion
             */
            if (type == EdgeType.STATIC_ATTR.get()
                    || type == EdgeType.DYNAMIC_ATTR.get()) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {
                    /**
                     * the vertex is stored locally
                     */
                    inst.localStore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();
                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                    }
                    //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                    throw re;

                }
                //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                return 0;
            }

            /**
             * Edge Insertion
             */
            if (!inst.split.containsKey(src) ||
                    (inst.split.containsKey(src) && inst.split.get(src) == 0)) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {

                    inst.localStore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();

                    inst.edgecounters.putIfAbsent(dst, new Counters());

                    if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()) {
                        inst.edgecounters.get(src).alo += 1;
                        inst.edgecounters.get(dst).ali += 1;
                    } else {
                        inst.edgecounters.get(src).plo += 1;
                        inst.edgecounters.get(dst).pli += 1;
                    }
                    inst.edgecounters.get(src).actual_exist = 1;
                    inst.edgecounters.get(dst).actual_exist = 1;

                    /**
                     * Check whether we need to reassign or split the vertex
                     */
                    c.edges += 1;

                    if (c.edges > Constants.SPLIT_THRESHOLD) {
                        // need to split src's edges;
                        GLogger.info("[%d] %s needs split", inst.getLocalIdx(), new String(bsrc));
                        try {
                            inst.spliter.splitVertex(src);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                        return 0;
                    }

                    if (c.edges > (Math.pow(2, c.reassign_times) * Constants.REASSIGN_THRESHOLD)) {
                        // Need to reassign again
                        GLogger.info("[%d] %s needs reassign", inst.getLocalIdx(), new String(bsrc));
                        c.reassign_times += 1;
                        try {
                            inst.reassigner.reassignVertex(src);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                        return 0;
                    }

                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                    }
                    //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                    throw re;
                }

            } else { //if src has been split

                if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()) {
                    inst.localStore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();

                } else {
                    /**
                     * Client is requesting a wrong server;
                     * Throw exception and ask them to re-try
                     */
                    RedirectException re = new RedirectException();

                    if (inst.getHashLocation(bdst, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(dst));
                    } else {
                        re.setStatus(Constants.EDGE_SPLIT_WRONG_SRV);
                    }

                    //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
                    throw re;

                }

            }
        }
        int mf = 0;
        for (Counters ctmp : inst.edgecounters.values())
            if (ctmp.actual_exist == 1) mf += 1;
        GLogger.info("real memory footprint: " + mf + " expected: " + inst.edgecounters.size());
        //GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "insert");
        return 0;
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public List<Dist> get_state() throws TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);

        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DBKey newKey = new DBKey(bsrc, bdst, type);

        inst.edgecounters.putIfAbsent(src, new Counters());
        Counters c = inst.edgecounters.get(src);

        synchronized (c) {
            /**
             * Initial Status. loc is empty and but current server is src's hash location
             * Return empty list
             */
            if (!inst.loc.containsKey(src) &&
                    inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx())
                return rtn;

            /*
             *  Read Vertex
             */
            if (type == EdgeType.STATIC_ATTR.get()
                    || type == EdgeType.DYNAMIC_ATTR.get()) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {
                    /**
                     * the vertex is stored locally
                     */
                    byte[] value = inst.localStore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));
                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                        throw re;
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                        throw re;
                    }
                }
                return rtn;
            }

            /**
             * Read Edge
             */
            if (!inst.split.containsKey(src) ||
                    (inst.split.containsKey(src) && inst.split.get(src) == 0)) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {

                    byte[] value = inst.localStore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));

                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                        throw re;
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                        throw re;
                    }
                }

            } else { //if src has been split

                if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()) {

                    byte[] value = inst.localStore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));

                } else {
                    /**
                     * Client is requesting a wrong server;
                     * Throw exception and ask them to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bdst, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(dst));
                        throw re;
                    } else {
                        re.setStatus(Constants.EDGE_SPLIT_WRONG_SRV);
                        throw re;
                    }

                }
            }
        }
        return rtn;
    }

    @Override
    public synchronized List<KeyValue> scan(ByteBuffer src, int type) throws TException {
        GLogger.info("[%d]-[START]-[%s]", inst.getLocalIdx(), "scan");

        byte[] bsrc = NIOHelper.getActiveArray(src);

        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);

        inst.edgecounters.putIfAbsent(src, new Counters());
        Counters c = inst.edgecounters.get(src);

        synchronized (c) {
            /**
             * Initial Status. loc is empty and but current server is src's hash location
             * Return empty list
             */
            if (!inst.loc.containsKey(src) &&
                    inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx())
                return rtn;

            if (type == EdgeType.STATIC_ATTR.get()
                    || type == EdgeType.DYNAMIC_ATTR.get())
                return rtn;

            /**
             * Scan Edge
             */
            if (!inst.split.containsKey(src) ||
                    (inst.split.containsKey(src) && inst.split.get(src) == 0)) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {
                    ArrayList<KeyValue> kvs = inst.localStore.scanKV(startKey.toKey(), endKey.toKey());
                    rtn.addAll(kvs);

                } else {
                    RedirectException re = new RedirectException();
                    if (inst.getHashLocation(bsrc, inst.serverNum) == inst.getLocalIdx()) {
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                        throw re;
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                        throw re;
                    }
                }
            } else { //if src has been split
                /**
                 * Ask clients to re-try (broadcast)
                 */
                RedirectException re = new RedirectException();
                re.setStatus(Constants.EDGE_SPLIT_WRONG_SRV);
                throw re;
            }
        }

        GLogger.info("[%d]-[END]-[%s]", inst.getLocalIdx(), "scan");
        return rtn;
    }

    @Override
    public synchronized List<KeyValue> iogp_force_scan(ByteBuffer src, int type) throws TException {
        GLogger.debug("[%d]-[START]-[%s]", inst.getLocalIdx(), "force_scan");

        byte[] bsrc = NIOHelper.getActiveArray(src);

        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);

        ArrayList<KeyValue> kvs = inst.localStore.scanKV(startKey.toKey(), endKey.toKey());
        rtn.addAll(kvs);

        GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "force_scan");
        return rtn;
    }


    @Override
    public int iogp_batch_insert(List<KeyValue> batches, int type) throws TException {
        GLogger.debug("[%d]-[START]-[%s]", inst.getLocalIdx(), "batch_insert");
        if (type == 0) { //Split phase uses this
            RedirectException re = new RedirectException();
            List<Movement> mvs = new ArrayList<>();
            for (KeyValue kv : batches) {
                DBKey key = new DBKey(kv.getKey());
                ByteBuffer bdst = ByteBuffer.wrap(key.dst);

                if (inst.loc.containsKey(bdst) && inst.loc.get(bdst) == inst.getLocalIdx()) {
                    inst.localStore.put(kv.getKey(), kv.getValue());
                    inst.size.getAndIncrement();
                } else {
                    if (!inst.loc.containsKey(bdst) &&
                            inst.getHashLocation(NIOHelper.getActiveArray(bdst),
                                    inst.serverNum) == inst.getLocalIdx())

                        inst.loc.put(bdst, inst.getLocalIdx());

                    mvs.add(new Movement(inst.loc.get(bdst), kv));
                }
            }
            if (!mvs.isEmpty()) {
                re.setRe(mvs);
                GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "batch_insert");
                throw re;
            }
            GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "batch_insert");
            return 0;
        }
        if (type == 1) {  //Vertex Reassign uses this
            for (KeyValue kv : batches) {
                inst.localStore.put(kv.getKey(), kv.getValue());
                inst.size.getAndIncrement();

                DBKey key = new DBKey(kv.getKey());
                ByteBuffer bdst = ByteBuffer.wrap(key.dst);
                inst.edgecounters.putIfAbsent(bdst, new Counters());
                Counters dstc = inst.edgecounters.get(bdst);

                if (inst.loc.containsKey(bdst)
                        && inst.loc.get(bdst) == inst.getLocalIdx()) {
                    if (key.type == EdgeType.OUT.get())
                        dstc.ali += 1;
                    if (key.type == EdgeType.IN.get())
                        dstc.alo += 1;
                }
            }
        }
        GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "batch_insert");
        return 0;
    }

    @Override
    public int iogp_split(ByteBuffer src) throws TException {
        GLogger.debug("[%d]-[START]-[%s]", inst.getLocalIdx(), "split");
        inst.edgecounters.remove(src);
        inst.split.put(src, 1);
        GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "split");
        return 0;
    }

    @Override
    public int iogp_reassign(ByteBuffer src, int type, int target) throws RedirectException, TException {
        GLogger.debug("[%d]-[START]-[%s]", inst.getLocalIdx(), "reassign");
        if (type == 0) { //update hash-loc
            inst.loc.put(src, target);
        } else {
            // when type > 0, it is actually showing how many times the vertex has been reassigned.
            inst.edgecounters.putIfAbsent(src, new Counters());
            Counters c = inst.edgecounters.get(src);
            c.alo = c.plo;
            c.ali = c.pli;
            c.reassign_times = type;
            inst.loc.put(src, target);
        }
        GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "reassign");

        return 0;
    }

    @Override
    public int iogp_fennel(ByteBuffer src) throws RedirectException {
        GLogger.debug("[%d]-[START]-[%s]", inst.getLocalIdx(), "fennel");
        inst.edgecounters.putIfAbsent(src, new Counters());
        Counters c = inst.edgecounters.get(src);
        int score = 2 * (c.pli + c.plo) - inst.size.get();
        GLogger.debug("[%d]-[END]-[%s]", inst.getLocalIdx(), "fennel");
        return score;
    }

    @Override
    public int iogp_syncstatus(List<Status> statuses) throws RedirectException, RedirectException {
        for (Status s : statuses) {
            ByteBuffer v = ByteBuffer.wrap(s.getKey());
            int is_split = s.getIssplit();
            int location = s.getLocation();
            inst.syncedLocationInfo.put(v, location);
            inst.syncedSplitInfo.put(v, is_split);
        }
        GLogger.info("Memory Consumption: " + inst.edgecounters.size());
        return 0;
    }



    @Override
    public int giga_batch_insert(ByteBuffer src, int vid, List<KeyValue> batches) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int giga_split(ByteBuffer src, int vid, int stage, ByteBuffer bitmap) throws TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int giga_rec_split(ByteBuffer src, int vid, List<KeyValue> batches) throws TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public GigaScan giga_scan(ByteBuffer src, int type, ByteBuffer bitmap) throws TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

}
