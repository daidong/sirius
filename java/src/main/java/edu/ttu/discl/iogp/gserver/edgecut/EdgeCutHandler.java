package edu.ttu.discl.iogp.gserver.edgecut;

import edu.ttu.discl.iogp.gserver.BaseHandler;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.tengine.SyncTravelEngine;
import edu.ttu.discl.iogp.thrift.Dist;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.RedirectException;
import edu.ttu.discl.iogp.thrift.Status;
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
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);
        
        DBKey newKey = new DBKey(bsrc, bdst, type);
        instance.localstore.put(newKey.toKey(), bval);
        return 0;
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        
        List<KeyValue> rtn = new ArrayList<KeyValue>();        
        DBKey newKey = new DBKey(bsrc, bdst, type);
        KeyValue p = instance.localstore.seekTo(newKey.toKey());
        if (p != null) {
            rtn.add(p);
        }
        return rtn;
    }

    @Override
    public synchronized List<KeyValue> scan(ByteBuffer src, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);

        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);
        ArrayList<KeyValue> kvs = instance.localstore.scanKV(startKey.toKey(), endKey.toKey());

        return kvs;
    }

    @Override
    public List<KeyValue> force_scan(ByteBuffer src, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws TException {
        instance.localstore.batch_put(batches);
        return Constants.RTN_SUCC;
    }

    @Override
    public List<Dist> get_state() throws TException {
        return null;
    }

    @Override
    public int split(ByteBuffer src) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    public int reassign(ByteBuffer src, int type, int target) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int fennel(ByteBuffer src) throws RedirectException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int syncstatus(List<Status> statuses) throws RedirectException, RedirectException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
