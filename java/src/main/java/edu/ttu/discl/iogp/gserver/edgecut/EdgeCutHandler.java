package edu.ttu.discl.iogp.gserver.edgecut;

import edu.ttu.discl.iogp.gserver.BaseHandler;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.tengine.SyncTravelEngine;
import edu.ttu.discl.iogp.thrift.Dist;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.Constants;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class EdgeCutHandler extends BaseHandler {

    public EdgeCutSrv instance = null;
    
    public EdgeCutHandler(EdgeCutSrv s) {
        this.instance = s;
        this.syncEngine = new SyncTravelEngine(s);
    }

    @Override
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val, long ts) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);
        
        DBKey newKey = new DBKey(bsrc, bdst, type, ts);
        instance.localstore.put(newKey.toKey(), bval);
        return 0;
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type, long ts) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        
        List<KeyValue> rtn = new ArrayList<KeyValue>();        
        DBKey newKey = new DBKey(bsrc, bdst, type, ts);
        KeyValue p = instance.localstore.seekTo(newKey.toKey()); //due to the time reason, we have to seek
        if (p != null) {
            rtn.add(p);
        }
        return rtn;
    }

    /**
     * scan: [start_ts, end_ts];
     */
    @Override
    public synchronized List<KeyValue> scan(ByteBuffer src, int type, ByteBuffer bitmap, long start, long end) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        
        List<KeyValue> rtn = new ArrayList<KeyValue>();
        boolean flag = false;

        if (start == end) { //reversed time! 
            flag = true;
            start = Long.MAX_VALUE;
            end = 0;
        } else {  //reversed time!
            long tmp = start;
            start = end;
            end = tmp;
        }

        DBKey startKey = DBKey.MinDBKey(bsrc, type, start);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type, end);
        ArrayList<KeyValue> kvs = instance.localstore.scanKV(startKey.toKey(), endKey.toKey());
        HashSet<ByteBuffer> contains = new HashSet<ByteBuffer>();

        for (KeyValue kv : kvs) {
            DBKey dbKey = new DBKey(kv.getKey());
            long currTs = dbKey.ts;
            ByteBuffer dst = ByteBuffer.wrap(dbKey.dst);

            if (currTs >= end && currTs <= start) { //reversed time
                if (flag) { //only return the newest version of all edges
                    if (!contains.contains(dst)) {
                        contains.add(dst);
                        rtn.add(kv);
                    }
                } else {
                    rtn.add(kv);
                }
            }
        }
        return rtn;
    }

    @Override
    public int batch_insert(ByteBuffer src, int vid, List<KeyValue> batches) throws TException {
        instance.localstore.batch_put(batches);
        return Constants.RTN_SUCC;
    }

    public int pre_checkin() throws TException {
        return 0;
    }

    @Override
    public List<Dist> get_state() throws TException {
        return null;
    }

    @Override
    public int split(ByteBuffer src, int vid, int stage, ByteBuffer bitmap) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public int rec_split(ByteBuffer src, int vid, List<KeyValue> batches) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

}
