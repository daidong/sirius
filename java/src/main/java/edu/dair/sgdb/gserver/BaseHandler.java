package edu.dair.sgdb.gserver;

import edu.dair.sgdb.tengine.SyncTravelEngine;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.thrift.*;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author daidong
 */
public abstract class BaseHandler implements TGraphFSServer.Iface {

    public SyncTravelEngine syncEngine = null;

    abstract public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val) throws RedirectException, TException;

    abstract public List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type) throws RedirectException, TException;

    abstract public List<KeyValue> scan(ByteBuffer src, int type) throws RedirectException, TException;

    abstract public List<KeyValue> force_scan(ByteBuffer src, int type) throws RedirectException, TException;

    abstract public int batch_insert(List<KeyValue> batches, int type) throws RedirectException, TException;

    abstract public List<Dist> get_state() throws TException;

    abstract public int split(ByteBuffer src) throws RedirectException, TException;

    abstract public int reassign(ByteBuffer src, int type, int target) throws RedirectException, TException;

    abstract public int fennel(ByteBuffer src) throws RedirectException, TException;

    abstract public int syncstatus(List<Status> statuses) throws RedirectException, TException;

    public int syncTravel(TravelCommand tc) throws TException {
        return syncEngine.syncTravel(tc);
    }

    public int syncTravelMaster(TravelCommand tc) throws TException {
        return syncEngine.syncTravelMaster(tc);
    }

    public int syncTravelRtn(TravelCommand tc) throws TException {
        return syncEngine.syncTravelRtn(tc);
    }

    public int syncTravelStart(TravelCommand tc) throws TException {
        return syncEngine.syncTravelStart(tc);
    }

    public int syncTravelExtend(TravelCommand tc) throws TException {
        return syncEngine.syncTravelExtend(tc);
    }

    public int syncTravelFinish(TravelCommand tc) throws TException {
        return syncEngine.syncTravelFinish(tc);
    }

    public int deleteSyncTravelInstance(TravelCommand tc) throws TException {
        return syncEngine.deleteSyncTravelInstance(tc);
    }

}
