package edu.dair.sgdb.gserver;

import edu.dair.sgdb.tengine.abfs.abfs;
import edu.dair.sgdb.tengine.async.AsyncTravelEngine;
import edu.dair.sgdb.tengine.bfs.bfs;
import edu.dair.sgdb.tengine.sync.SyncTravelEngine;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.JenkinsHash;

import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author daidong
 */
public abstract class BaseHandler implements TGraphFSServer.Iface {

    public AsyncTravelEngine asyncEngine = null;
    public SyncTravelEngine syncEngine = null;
    public bfs bfs_engine = null;
    public abfs abfs_engine = null;

    public int echo(int s, ByteBuffer payload) throws TException{
        byte[] load = NIOHelper.getActiveArray(payload);
        System.out.println("[Thrift Test] Server Receive: " + " Int: " + s + " ByteBuffer: " + new String(load));
        return 0;
    }

    public int travel(TravelCommand tc) throws TException{
        return asyncEngine.travel(tc);
    }

    public int travelMaster(TravelCommand tc) throws TException {
        return asyncEngine.travelMaster(tc);
    }

    public int travelRtn(TravelCommand tc) throws TException {
        return asyncEngine.travelRtn(tc);
    }

    public int travelReg(TravelCommand tc) throws TException {
        return asyncEngine.travelReg(tc);
    }

    public int travelFin(TravelCommand tc) throws TException {
        return asyncEngine.travelFin(tc);
    }

    public int deleteTravelInstance(TravelCommand tc) throws TException {
        return asyncEngine.deleteTravelInstance(tc);
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

    protected int getHashLocation(byte[] src, int serverNum) {
        JenkinsHash jh = new JenkinsHash();
        int hashi = Math.abs(jh.hash32(src));
        return (hashi % serverNum);
    }


    @Override
    public int travel_master(long tid, String payload) throws TException {
        return bfs_engine.travel_master(tid, payload);
    }

    @Override
    public int travel_vertices(long tid, int sid, Set<ByteBuffer> keys, String payload) throws TException {
        return bfs_engine.travel_vertices(tid, sid, keys, payload);
    }

    @Override
    public Set<ByteBuffer> travel_edges(long tid, int sid, Set<ByteBuffer> keys, String payload) throws TException {
        return bfs_engine.travel_edges(tid, sid, keys, payload);
    }

    @Override
    public Set<Integer> travel_start_step(long tid, int sid, String payload) throws TException {
        return bfs_engine.travel_start_step(tid, sid, payload);
    }


    @Override
    public int async_travel_master(long tid, String payload) throws TException {
        return abfs_engine.async_travel_master(tid, payload);
    }

    @Override
    public int async_travel_vertices(long tid, int sid, Set<ByteBuffer> keys,
                                     long uuid, int master_id, String payload) throws TException {
        return abfs_engine.async_travel_vertices(tid, sid, keys, uuid, master_id, payload);
    }

    @Override
    public int async_travel_edges(long tid, int sid, Set<ByteBuffer> keys,
                                  long uuid, int master_id, String payload) throws TException {
        return abfs_engine.async_travel_edges(tid, sid, keys, uuid, master_id, payload);
    }

    @Override
    public int async_travel_report(long tid, int sid, Set<Long> uuids, int type) throws TException {
        return abfs_engine.async_travel_report(tid, sid, uuids, type);
    }

}
