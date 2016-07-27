package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.gserver.BaseHandler;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.tengine.SyncTravelEngine;
import edu.ttu.discl.iogp.thrift.Dist;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.Movement;
import edu.ttu.discl.iogp.thrift.RedirectException;
import edu.ttu.discl.iogp.utils.Constants;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class IOGPHandler extends BaseHandler {

    public IOGPSrv inst = null;

    public IOGPHandler(IOGPSrv s) {
        this.inst = s;
        this.syncEngine = new SyncTravelEngine(s);
    }

    @Override
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);

        DBKey newKey = new DBKey(bsrc, bdst, type);

        inst.edgecounters.putIfAbsent(src, new Counters(Constants.REASSIGN_THRESHOLD));
        Counters c = inst.edgecounters.get(src);

        synchronized (c) {
			/**
			 * Initial Status. loc is empty and but current server is src's hash location
             */
            if (!inst.loc.containsKey(src) &&
                    inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx()){
                inst.loc.put(src, inst.getLocalIdx());
                inst.split.put(src, 0);
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
                    inst.localstore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();
                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx()){
                        re.setStatus(Constants.RE_ACTUAL_LOC);
                        re.setTarget(inst.loc.get(src));
                        throw re;
                    } else {
                        re.setStatus(Constants.RE_VERTEX_WRONG_SRV);
                        throw re;
                    }
                }
                return 0;
            }

            /**
             * Edge Insertion
             */
            if (!inst.split.containsKey(src) ||
                    (inst.split.containsKey(src) && inst.split.get(src) == 0)) {

                if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()) {

                    inst.localstore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();

                    inst.edgecounters.putIfAbsent(dst, new Counters(Constants.REASSIGN_THRESHOLD));

                    if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()) {
                        inst.edgecounters.get(src).alo += 1;
                        inst.edgecounters.get(dst).ali += 1;
                    } else {
                        inst.edgecounters.get(src).plo += 1;
                        inst.edgecounters.get(dst).pli += 1;
                    }

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
                        return 0;
                    }

                    if (c.edges > c.last_reassign_threshold) {
                        // Need to reassign again
                        GLogger.info("[%d] %s needs reassign", inst.getLocalIdx(), new String(bsrc));
                        c.last_reassign_threshold = 2 * c.last_reassign_threshold;
                        try {
                            inst.reassigner.reassignVertex(src);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return 0;
                    }

                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx()){
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
                    inst.localstore.put(newKey.toKey(), bval);
                    inst.size.getAndIncrement();
                } else {
                    /**
                     * Client is requesting a wrong server;
                     * Throw exception and ask them to re-try
                     */
                    RedirectException re = new RedirectException();

                    if (inst.getEdgeLoc(bdst, inst.serverNum) == inst.getLocalIdx()){
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

        return 0;
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);

        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DBKey newKey = new DBKey(bsrc, bdst, type);

        inst.edgecounters.putIfAbsent(src, new Counters(Constants.REASSIGN_THRESHOLD));
        Counters c = inst.edgecounters.get(src);

        synchronized (c) {
            /**
             * Initial Status. loc is empty and but current server is src's hash location
             * Return empty list
             */
            if (!inst.loc.containsKey(src) &&
                    inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx())
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
                    byte[] value = inst.localstore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));
                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx()){
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

                    byte[] value = inst.localstore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));

                } else {
                    /**
                     * The client is requesting a wrong server,
                     * thrown exception and ask client to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getEdgeLoc(bsrc, inst.serverNum) == inst.getLocalIdx()){
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

                    byte[] value = inst.localstore.get(newKey.toKey());
                    rtn.add(new KeyValue(ByteBuffer.wrap(newKey.toKey()),
                            ByteBuffer.wrap(value)));

                } else {
                    /**
                     * Client is requesting a wrong server;
                     * Throw exception and ask them to re-try
                     */
                    RedirectException re = new RedirectException();
                    if (inst.getEdgeLoc(bdst, inst.serverNum) == inst.getLocalIdx()){
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
        byte[] bsrc = NIOHelper.getActiveArray(src);
        
        List<KeyValue> rtn = new ArrayList<KeyValue>();

        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);
        ArrayList<KeyValue> kvs = inst.localstore.scanKV(startKey.toKey(), endKey.toKey());
        HashSet<ByteBuffer> contains = new HashSet<ByteBuffer>();

        for (KeyValue kv : kvs)
            rtn.add(kv);

        return rtn;
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws TException {
        if (type == 0) { //Split phase uses this
            RedirectException re = new RedirectException();
            List<Movement> mvs = new ArrayList<>();
            for (KeyValue kv : batches) {
                DBKey key = new DBKey(kv.getKey());
                ByteBuffer bdst = ByteBuffer.wrap(key.dst);
                if (inst.loc.containsKey(bdst) && inst.loc.get(bdst) == inst.getLocalIdx()) {
                    inst.localstore.put(kv.getKey(), kv.getValue());
                    inst.size.getAndIncrement();
                } else {
                    mvs.add(new Movement(inst.loc.get(bdst), kv));
                }
            }
            if (!mvs.isEmpty()) {
                re.setRe(mvs);
                throw re;
            }
            return 0;
        }
        if (type == 1){  //Vertex Reassign uses this
            for (KeyValue kv : batches){
                inst.localstore.put(kv.getKey(), kv.getValue());
                inst.size.getAndIncrement();

                DBKey key = new DBKey(kv.getKey());
                ByteBuffer bdst = ByteBuffer.wrap(key.dst);
                if (!inst.edgecounters.containsKey(bdst))
                    inst.edgecounters.put(bdst, new Counters(Constants.REASSIGN_THRESHOLD));
                Counters dstc = inst.edgecounters.get(bdst);

                if (inst.loc.containsKey(bdst)
                        && inst.loc.get(bdst) == inst.getLocalIdx()){
                    if (key.type == EdgeType.OUT.get())
                        dstc.ali += 1;
                    if (key.type == EdgeType.IN.get())
                        dstc.alo += 1;
                }
            }
        }
        return 0;
    }

    @Override
    public int split(ByteBuffer src) throws TException{
        inst.edgecounters.remove(src);
        inst.split.put(src, 1);
        return 0;
    }

    @Override
    public int reassign(ByteBuffer src, int type, int target) throws RedirectException, TException {
        if (type == 0){ //update hash-loc
            inst.loc.put(src, target);
        }
        if (type == 1){
            inst.edgecounters.putIfAbsent(src, new Counters(Constants.REASSIGN_THRESHOLD));
            Counters c = inst.edgecounters.get(src);
            c.alo = c.plo;
            c.ali = c.pli;
            inst.loc.put(src, target);
        }
        return 0;
    }

    @Override
    public int fennel(ByteBuffer src) throws RedirectException {
        inst.edgecounters.putIfAbsent(src, new Counters(Constants.REASSIGN_THRESHOLD));
        Counters c = inst.edgecounters.get(src);
        int score = 2 * (c.pli + c.plo) - inst.size.get();
        return score;
    }

    @Override
    public List<Dist> get_state() throws TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
