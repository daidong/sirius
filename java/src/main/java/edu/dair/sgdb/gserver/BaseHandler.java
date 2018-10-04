package edu.dair.sgdb.gserver;

import edu.dair.sgdb.tengine.AsyncTravelEngine;
import edu.dair.sgdb.tengine.SyncTravelEngine;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.JenkinsHash;

import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author daidong
 */
public abstract class BaseHandler implements TGraphFSServer.Iface {

    public AsyncTravelEngine asyncEngine = null;
    public SyncTravelEngine syncEngine = null;

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

}
