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
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class IOGPHandler extends BaseHandler {

    public IOGPSrv inst = null;

    public final static int RE_VERTEX_WRONG_SRV = 0;
    public final static int RE_EDGE_REASSIGN_WRONG_SRV = 1;
    public final static int RE_EDGE_SPLIT_WRONG_SRV = 2;

    public final static int SPLIT_THRESHOLD = 1000;
    public final static int REASSIGN_THRESHOLD = 100;

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

        /*
         *  Vertex Insertion
         */
        if (type == EdgeType.STATIC_ATTR.get()
                || type == EdgeType.DYNAMIC_ATTR.get()){

            // if the vertex is stored locally
            if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()){
                inst.localstore.put(newKey.toKey(), bval);
                inst.size += 1;
            } else {
                /**
                 * The client is requesting a wrong server, thrown exception and ask client to re-try
                 */
                RedirectException re = new RedirectException();
                re.setStatus(RE_VERTEX_WRONG_SRV);
                throw re;
            }
            return 0;
        }

		/**
         * Edge Insertion
         */
        // Whether src has not been split yet
        if (inst.split.containsKey(src) && inst.split.get(src) == 0){

            if (inst.loc.containsKey(src) && inst.loc.get(src) == inst.getLocalIdx()){

                inst.localstore.put(newKey.toKey(), bval);
                inst.size += 1;

                if (!inst.edgecounters.containsKey(src)) {
                    inst.edgecounters.put(src, new Counters(REASSIGN_THRESHOLD));
                }
                if (!inst.edgecounters.containsKey(dst)) {
                    inst.edgecounters.put(dst, new Counters(REASSIGN_THRESHOLD));
                }

                if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()){
                    inst.edgecounters.get(src).alo += 1;
                    inst.edgecounters.get(dst).ali += 1;
                } else {
                    inst.edgecounters.get(src).plo += 1;
                    inst.edgecounters.get(dst).pli += 1;
                }

				/**
				 * Check whether we need to reassign or split the vertex
                 */
                Counters c = inst.edgecounters.get(src);
                c.edges += 1;

                if (c.edges > SPLIT_THRESHOLD){
                    // need to split src's edges;
                    try {
                        inst.spliter.splitVertex(src);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (c.edges > c.last_reassign_threshold * 2){
                    // Need to reassign again
                    c.last_reassign_threshold = 2 * c.last_reassign_threshold;
                    //@TODO: Use Fennel to decide and move data
                    try {
                        inst.reassigner.reassignVertex(src);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            } else {
                /**
                 * The client is requesting a wrong server, thrown exception and ask client to re-try
                 */
                RedirectException re = new RedirectException();
                re.setStatus(RE_EDGE_REASSIGN_WRONG_SRV);
                throw re;
            }

        } else { //if src has been split

            if (inst.loc.containsKey(dst) && inst.loc.get(dst) == inst.getLocalIdx()){
                inst.localstore.put(newKey.toKey(), bval);
                inst.size += 1;
            } else {
				/**
				 * Client is requesting a wrong server; Throw exception and ask them to re-try
                 */
                RedirectException re = new RedirectException();
                re.setStatus(RE_EDGE_SPLIT_WRONG_SRV);
                throw re;
            }

        }

        return 0;
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        throw new UnsupportedOperationException("Not supported yet.");
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
        RedirectException re = new RedirectException();
        List<Movement> mvs = new ArrayList<>();
        for (KeyValue kv : batches){
            DBKey key = new DBKey(kv.getKey());
            ByteBuffer bsrc = ByteBuffer.wrap(key.src);
            if (inst.loc.containsKey(bsrc) && inst.loc.get(bsrc) == inst.getLocalIdx()){
                inst.localstore.put(kv.getKey(), kv.getValue());
                inst.size += 1;
            } else {
                mvs.add(new Movement(inst.loc.get(bsrc), kv));
            }
        }
        if (!mvs.isEmpty()) {
            re.setRe(mvs);
            throw re;
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
    public int reassign(ByteBuffer src, int type) throws RedirectException, TException {
        return 0;
    }

    @Override
    public int fennel(ByteBuffer src) throws RedirectException, RedirectException {
        return 0;
    }

    @Override
    public List<Dist> get_state() throws TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
