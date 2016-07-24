package edu.ttu.discl.iogp.gserver;

import edu.ttu.discl.iogp.tengine.SyncTravelEngine;
import edu.ttu.discl.iogp.thrift.*;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author daidong
 */
public abstract class BaseHandler implements TGraphFSServer.Iface{

    public SyncTravelEngine syncEngine = null;

    /**
     * insert an edge into the storage.
     * @param src
     * @param dst
     * @param type
     * @param val
     * @param ts
     * @return
     * @throws RedirectException
     * @throws TException
     */
    abstract public int insert(ByteBuffer src, ByteBuffer dst, int type, ByteBuffer val, long ts) throws RedirectException, TException;

    /**
     * batch insert the edge into the storage.
     * @param src
     * @param vid
     * @param batches
     * @return
     * @throws RedirectException
     * @throws TException
     */
    abstract public int batch_insert(ByteBuffer src, int vid, List<KeyValue> batches) throws RedirectException, TException;

    /**
     * split the edge
     * @param src
     * @param vid
     * @param stage
     * @param bitmap
     * @return
     * @throws TException
     */
    abstract public int split(ByteBuffer src, int vid, int stage, ByteBuffer bitmap) throws TException;

    /**
     * receive splitted key value.
     * @param src
     * @param vid
     * @param batches
     * @return
     * @throws TException
     */
    abstract public int rec_split(ByteBuffer src, int vid, List<KeyValue> batches) throws TException;

    abstract public List<Dist> get_state() throws TException;

    /**
     * read information of a certain edge.
     * @param src
     * @param dst
     * @param type
     * @param ts
     * @return
     * @throws RedirectException
     * @throws TException
     */
    abstract public List<KeyValue> read(ByteBuffer src, ByteBuffer dst, int type, long ts) throws RedirectException, TException;

    /**
     * scan all the out-going edges from src.
     * @param src
     * @param type
     * @param bitmap
     * @param ts1
     * @param ts2
     * @return
     * @throws RedirectException
     * @throws TException
     */
    abstract public List<KeyValue> scan(ByteBuffer src, int type, ByteBuffer bitmap, long ts1, long ts2) throws RedirectException, TException;
    
    public int echo(int s, ByteBuffer payload) throws TException{
        byte[] load = NIOHelper.getActiveArray(payload);
        System.out.println("[Thrift Test] Server Receive: " + " Int: " + s + " ByteBuffer: " + new String(load));
        return 0;
    }
    
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
