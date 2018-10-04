package edu.dair.sgdb.gserver.vertexcut;

import edu.dair.sgdb.gserver.BaseHandler;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.tengine.SyncTravelEngine;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class VertexCutHandler extends BaseHandler {

    public VertexCutSrv instance = null;

    public VertexCutHandler(VertexCutSrv s) {
        this.instance = s;
        this.syncEngine = new SyncTravelEngine(s);
        //this.asyncEngine = new AsyncTravelEngine(s);
    }

    @Override
    public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws RedirectException, TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);
        byte[] bval = NIOHelper.getActiveArray(val);

        DBKey newKey = new DBKey(bsrc, bdst, type);
        instance.localStore.put(newKey.toKey(), bval);
        return 0;
    }

    @Override
    public List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws RedirectException, TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        byte[] bdst = NIOHelper.getActiveArray(dst);

        List<KeyValue> rtn = new ArrayList<KeyValue>();
        DBKey newKey = new DBKey(bsrc, bdst, type);
        KeyValue p = instance.localStore.seekTo(newKey.toKey()); //due to the time reason, we have to seek
        if (p != null) {
            rtn.add(p);
        }
        return rtn;
    }

    @Override
    public List<KeyValue> scan(ByteBuffer src, int type) throws RedirectException, TException {
        byte[] bsrc = NIOHelper.getActiveArray(src);

        List<KeyValue> rtn = new ArrayList<KeyValue>();
        boolean flag = false;

        DBKey startKey = DBKey.MinDBKey(bsrc, type);
        DBKey endKey = DBKey.MaxDBKey(bsrc, type);
        ArrayList<KeyValue> kvs = instance.localStore.scanKV(startKey.toKey(), endKey.toKey());

        return rtn;
    }

    @Override
    public GigaScan giga_scan(ByteBuffer src, int type) throws TException {
        return null;
    }

    @Override
    public List<KeyValue> force_scan(ByteBuffer src, int type) throws RedirectException, TException {
        return null;
    }

    @Override
    public int batch_insert(List<KeyValue> batches, int type) throws RedirectException, TException {
        instance.localStore.batch_put(batches);
        return Constants.RTN_SUCC;
    }

    @Override
    public List<Dist> get_state() throws TException {
        return null;
    }

    @Override
    public int split(ByteBuffer src) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int reassign(ByteBuffer src, int type, int target) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int fennel(ByteBuffer src) throws RedirectException, TException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int syncstatus(List<Status> statuses) throws RedirectException, TException {
        return 0;
    }
}
