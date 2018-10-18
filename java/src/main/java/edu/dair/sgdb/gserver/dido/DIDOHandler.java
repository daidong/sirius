package edu.dair.sgdb.gserver.dido;

import edu.dair.sgdb.gserver.BaseHandler;
import edu.dair.sgdb.partitioner.DIDOIndex;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.tengine.abfs.abfs;
import edu.dair.sgdb.tengine.async.AsyncTravelEngine;
import edu.dair.sgdb.tengine.bfs.bfs;
import edu.dair.sgdb.tengine.sync.SyncTravelEngine;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.JenkinsHash;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DIDOHandler extends BaseHandler {

    public DIDOSrv instance;

    /**
     * Constructor of DIDOHandler.
     * The DIDOSrv instance must be firstly initialized.
     * The synchronized and asynchronized TravelEngine is also initialized with DIDOSrv instance.
     *
     * @param s  IncrGigaServer instance.
     */
    public DIDOHandler(DIDOSrv s) {
        this.instance = s;
        this.bfs_engine = new bfs(s);
        this.abfs_engine = new abfs(s);
        this.syncEngine = new SyncTravelEngine(s);
        this.asyncEngine = new AsyncTravelEngine(s);
    }

    /**
     * index : the virtual node ID.
     * server: get server ID from virtual node ID.
     *
     * 1. Calculate the hash value of dst vertex.
     * 2.
     * @param src
     * @param dst
     * @return
     *
     *
     */
    private int isLocalAndGetIndex(byte[] src, byte[] dst) {
        JenkinsHash jh = new JenkinsHash();
        int dstHash = Math.abs(jh.hash32(dst));
        DIDOIndex gi = instance.surelyGetGigaMap(src);
        int index = 0;
        int server = 0;
        index = gi.giga_get_index_for_hash(dstHash);
        server = gi.giga_get_server_from_index(index);
        if (server != instance.getLocalIdx()) {
            return -1;
        }
        return index;
    }

    /**
     * Given an edge, we
     * @param src
     * @param dst
     * @return
     */
    private int getIndex(byte[] src, byte[] dst) {
        JenkinsHash jh = new JenkinsHash();
        int dstHash = Math.abs(jh.hash32(dst));
        DIDOIndex gi = instance.surelyGetGigaMap(src);
        int index = gi.giga_get_index_for_hash(dstHash);
        return index;
    }

    public void persistentGigaIndex(byte[] src, DIDOIndex gi) {
        DBKey dbMetaKey = new DBKey(Constants.DB_META.getBytes(), src, 0);
        instance.localStore.put(dbMetaKey.toKey(), gi.toByteArray());
    }

    @Override
    public List<Dist> get_state() throws TException {
        HashMap<Integer, Integer> Split2VertexMap = new HashMap<Integer, Integer>();
        for (ByteBuffer src : instance.gigaMaps.keySet()) {
            byte[] bsrc = NIOHelper.getActiveArray(src);
            int startIdx = getHashLocation(bsrc, Constants.MAX_VIRTUAL_NODE);
            int physicalIdx = startIdx % instance.serverNum;

            if (physicalIdx == instance.getLocalIdx()) { //we only calculate vertices that should stored locally
                DIDOIndex gi = instance.gigaMaps.get(src);
                int splits = gi.getSplitCounter();
                if (!Split2VertexMap.containsKey(splits)) {
                    Split2VertexMap.put(splits, 0);
                }
                int v = Split2VertexMap.get(splits);
                Split2VertexMap.put(splits, (v + 1));
            }
        }
        ArrayList<Dist> rtn = new ArrayList<>();
        for (int split : Split2VertexMap.keySet()) {
            int num = Split2VertexMap.get(split);
            Dist dst = new Dist();
            dst.setSplitNum(split);
            dst.setVertexNum(num);
            rtn.add(dst);
        }
        return rtn;
    }

    @Override
    public int giga_split(ByteBuffer src, int vid, int stage, ByteBuffer bm) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);

        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);
        if (stage == Constants.SPLIT_START) {
            byte[] bbitmap = NIOHelper.getActiveArray(bm);
            gi.giga_update_bitmap(bbitmap);
            DIDOIndex.VirtualNodeStatus vns = gi.surelyGetVirtNodeStatus(vid);
            vns.split_from();

        } else if (stage == Constants.SPLIT_END) {
            DIDOIndex.VirtualNodeStatus vns = gi.surelyGetVirtNodeStatus(vid);
            vns.finish_split_from();

        } else if (stage == Constants.REPORT_SPLIT) {
            byte[] bbitmap = NIOHelper.getActiveArray(bm);
            gi.giga_update_bitmap(bbitmap);

        }
        persistentGigaIndex(bsrc, gi);
        return Constants.RTN_SUCC;
    }

    @Override
    public int giga_rec_split(ByteBuffer src, int vid, List<KeyValue> batches) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);

        for (KeyValue kv : batches) {
            instance.localStore.put(kv.getKey(), kv.getValue());
        }
        //instance.localStore.batch_put(batches);

        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);
        DIDOIndex.VirtualNodeStatus vns = gi.surelyGetVirtNodeStatus(vid);
        synchronized (vns) {
            vns.incr_size(batches.size());
        }
        //do not persistent gigaindex because it does not change
        //persistentGigaIndex(src, gi);
        return Constants.RTN_SUCC;
    }

    public int pre_checkin() throws TException {
        return 0;
    }

    @Override
    public int giga_batch_insert(ByteBuffer src, int vid, List<KeyValue> batches) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);
        DIDOIndex.VirtualNodeStatus vns = gi.surelyGetVirtNodeStatus(vid);

        synchronized(gi) {
            synchronized (vns) {
                if (vns.has_split()) {
                    System.out.println("Batch_Insert Into A Virtual Node Has Split, Fatal Error");
                }
                instance.localStore.batch_put(batches);
                vns.incr_size(batches.size());
            }

        }
        return Constants.RTN_SUCC;
    }

    @Override
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);

        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);
        int index = 0, vid = -1, new_index = -1, new_server = -1, new_vid = -1;
        DIDOIndex.VirtualNodeStatus vns = null;
        boolean needSplit = false;
        int vertexRootServer = gi.startServer % gi.serverNum;

        synchronized (gi) {

            if ((index = isLocalAndGetIndex(bsrc, bdst)) < 0) {
                RedirectException ge = new RedirectException();
                ge.setBitmap(gi.bitmap);
                throw ge;
            }

            vid = gi.giga_get_vid_from_index(index);
            vns = gi.surelyGetVirtNodeStatus(vid);

            synchronized (vns) {  //lock to avoid conflicts with SplitWorker thread.
                if (vns.is_splitting_to()) {
                    // we are currently splitting this virtual node; ignore the new element, put it into counters, let the SplitWorker
                    // calculates the correct size, and let next insertion decide whether to split it again.
                    vns.incr_counter();
                } else {
                    // we are not splitting this virtual node, do normal splitting.
                    vns.incr_size();
                    if (vns.size >= Constants.Count_Threshold && gi.giga_can_split(index)) {
                        new_index = gi.giga_split_mapping(index);
                        new_vid = gi.giga_get_vid_from_index(new_index);
                        new_server = gi.giga_get_server_from_index(new_index);
                        needSplit = true;

                        vns.split_to();
                        DIDOIndex.VirtualNodeStatus nvns = gi.surelyGetVirtNodeStatus(new_vid);
                        nvns.split_from();
                        // persistent GigaIndex once it has changed.
                        persistentGigaIndex(bsrc, gi);
                    }
                }
            }

            if (needSplit && (new_server != instance.getLocalIdx())) {
                TGraphFSServer.Client targetServer = instance.getClientConn(new_server);
                TGraphFSServer.Client rootServer = instance.getClientConn(vertexRootServer);

                try {
                    synchronized (targetServer) {
                        targetServer.giga_split(src, new_vid, Constants.SPLIT_START, ByteBuffer.wrap(gi.bitmap));
                    }
                    synchronized (rootServer) {
                        rootServer.giga_split(src, new_vid, Constants.SPLIT_START, ByteBuffer.wrap(gi.bitmap));
                    }
                } catch (TException e) {
                    GLogger.info("[%d] Send Split Command Exception", instance.getLocalIdx());
                }

                instance.releaseClientConn(new_server, targetServer);
                instance.releaseClientConn(vertexRootServer, rootServer);
                instance.workerPool.execute(new SplitWorker(instance, bsrc, vid, new_index, new_server, new_vid));
            }
        }

        // @TODO: Not check yet. We move the actual writing out of synchronization block to save some time.
        DBKey newKey = new DBKey(bsrc, bdst, type);
        instance.localStore.put(newKey.toKey(), bval);

        //instance.METRICS.meter("IncrGigaInsert").mark();
        return Constants.RTN_SUCC;
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    public class SplitWorker implements Runnable {

        private byte[] src;
        private DIDOSrv instance;
        private int currVid;
        private int newIndex;
        private int server;
        private int newVid;

        public SplitWorker(DIDOSrv s, byte[] src, int vid, int targetIndex, int targetServer, int targetVirtualNode) {
            this.instance = s;
            this.src = src;
            this.currVid = vid;
            this.newIndex = targetIndex;
            this.server = targetServer;
            this.newVid = targetVirtualNode;
        }

        @Override
        public void run() {
            DIDOIndex gi = instance.surelyGetGigaMap(src);
            DIDOIndex.VirtualNodeStatus currVns = gi.surelyGetVirtNodeStatus(currVid);
            DIDOIndex.VirtualNodeStatus newVns = gi.surelyGetVirtNodeStatus(newVid);
            JenkinsHash jh = new JenkinsHash();


            DBKey startKey = DBKey.MinDBKey(src);
            DBKey endKey = DBKey.MaxDBKey(src);

            ArrayList<KeyValue> vals = new ArrayList<KeyValue>();
            byte[] cur = instance.localStore.scanLimitedRes(startKey.toKey(), endKey.toKey(), Constants.LIMITS, vals);
            ArrayList<byte[]> removedKeys = new ArrayList<>();
            while (cur != null) {
                ArrayList<KeyValue> movs = new ArrayList<KeyValue>();
                for (KeyValue kv : vals) {
                    DBKey dbKey = new DBKey(kv.getKey());
                    byte[] dstKey = dbKey.dst;
                    int distHash = Math.abs(jh.hash32(dstKey));
                    //GigaIndex.pprint(instance.localIdx, gi.bitmap);
                    if (gi.giga_should_move(distHash, newIndex)) {
                        //GLogger.info("[%d] ---SEND--> %s to server[%d]", instance.localIdx,
                        //													new String(dstKey),
                        //													this.server);
                        movs.add(kv);
                    }
                }

                /*
                try {
                    Client target = instance.getClientConn(server);
                    synchronized (target) {
                        target.batch_insert(ByteBuffer.wrap(this.src), this.newVid, movs);
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }
                */
                for (KeyValue p : movs) {
                    removedKeys.add(p.getKey());
                }

                vals.clear();
                movs.clear();
                cur = instance.localStore.scanLimitedRes(cur, endKey.toKey(), Constants.LIMITS, vals);
            }

            synchronized (currVns) {
                currVns.decr_size(removedKeys.size());
                currVns.incr_size(currVns.countsWhileSplitting);
                currVns.reset_counter();
                currVns.finish_split_to();
                persistentGigaIndex(this.src, gi);
            }
            /**
             * Tell target server the splitting has finished.
             */
            newVns.finish_split_from();
            try {
                TGraphFSServer.Client target = instance.getClientConn(server);
                synchronized (target) {
                    target.giga_split(ByteBuffer.wrap(src), newVid, Constants.SPLIT_END, null);
                }
                instance.releaseClientConn(server, target);
            } catch (TException e) {
                e.printStackTrace();
            }

            /*
            //Make sure we reset splitting status, then delete local copies.
            for (byte[] k : removedKeys) {
                instance.localStore.remove(k);
                //GLogger.info("[%d] ---DELETE--> %s", instance.localIdx, new String(new DBKey(k).dst));
            }
            */
        }
    }

    @Override
    public List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);

        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);

        synchronized (gi) {
            int index = getIndex(bsrc, bdst);
            int server = gi.giga_get_server_from_index(index);
            int vid = gi.giga_get_vid_from_index(index);
            DIDOIndex.VirtualNodeStatus vns = gi.surelyGetVirtNodeStatus(vid);

            int p_index = gi.get_parent_index(index);
            int p_vid = gi.giga_get_vid_from_index(p_index);
            DIDOIndex.VirtualNodeStatus pvns = gi.surelyGetVirtNodeStatus(p_vid);

            if (server != instance.getLocalIdx()) {

                synchronized (vns) {
                    if (vns.is_splitting_from() && pvns.is_splitting_to()) {
                        //GLogger.info("[%d] %s:%s vns[%d] <--- p_vns[%d]", instance.localIdx, new String(src), new String(dst), vid, p_vid);
                        DBKey newKey = new DBKey(bsrc, bdst, type);
                        KeyValue val = instance.localStore.seekTo(newKey.toKey());
                        if (val != null) {
                            rtn.add(val);
                            return rtn;
                        } else {
                            RedirectException ge = new RedirectException();
                            ge.setBitmap(gi.bitmap);
                            throw ge;
                        }
                    } else {
                        RedirectException ge = new RedirectException();
                        ge.setBitmap(gi.bitmap);
                        throw ge;
                    }
                }
            } else {
                DBKey newKey = new DBKey(bsrc, bdst, type);

                while (vns.is_splitting_from()) {
                    //Data has not finished its moving, just hold until it finishes. TODO: can read from source.
                    KeyValue val = instance.localStore.seekTo(newKey.toKey());
                    if (val == null) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } else {
                        rtn.add(val);
                        return rtn;
                    }
                    //GLogger.info("[%d] Read %s:%s, Wait for 50ms until split finishes", instance.localIdx, new String(src), new String(dst));
                }

                KeyValue val = instance.localStore.seekTo(newKey.toKey());
                if (val != null) {
                    rtn.add(val);
                }

                return rtn;
            }
        }
    }

    @Override
    public List<KeyValue> scan(ByteBuffer src, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public GigaScan giga_scan(ByteBuffer src, int type, ByteBuffer bitmap) throws TException {
        GigaScan gs = new GigaScan();

        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bbitmap = NIOHelper.getActiveArray(bitmap);


        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);

        ArrayList<KeyValue> kvs = instance.localStore.scanKV(startKey.toKey(), endKey.toKey());
        DIDOIndex gi = instance.surelyGetGigaMap(bsrc);

        gs.setKvs(kvs);
        gs.setBitmap(gi.bitmap);

        return gs;
    }

    @Override
    public int iogp_batch_insert(List<KeyValue> batches, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public List<KeyValue> iogp_force_scan(ByteBuffer src, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int iogp_split(ByteBuffer src) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int iogp_reassign(ByteBuffer src, int type, int target) throws TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int iogp_fennel(ByteBuffer src) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

    @Override
    public int iogp_syncstatus(List<Status> statuses) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported by this server.");
    }

}