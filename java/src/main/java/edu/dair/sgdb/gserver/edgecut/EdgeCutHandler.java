package edu.dair.sgdb.gserver.edgecut;

import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.tengine.SyncTravelEngine;
import edu.dair.sgdb.gserver.BaseHandler;
import edu.dair.sgdb.thrift.Dist;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.RedirectException;
import edu.dair.sgdb.thrift.Status;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
        instance.localStore.put(newKey.toKey(), bval);
        return 0;
    }

    @Override
    public synchronized List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);

        List<KeyValue> rtn = new ArrayList<KeyValue>();
        DBKey newKey = new DBKey(bsrc, bdst, type);
        KeyValue p = instance.localStore.seekTo(newKey.toKey());
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
        ArrayList<KeyValue> kvs = instance.localStore.scanKV(startKey.toKey(), endKey.toKey());

        return kvs;
    }

    @Override
    public List<KeyValue> force_scan(ByteBuffer src, int type) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws TException {
        instance.localStore.batch_put(batches);
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
        return 0;
    }

}
