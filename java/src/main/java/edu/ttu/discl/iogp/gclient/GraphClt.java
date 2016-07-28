package edu.ttu.discl.iogp.gclient;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.tengine.JSONCommand;
import edu.ttu.discl.iogp.tengine.travel.SingleStep;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.thrift.TravelCommand;
import edu.ttu.discl.iogp.thrift.TravelCommandType;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.JenkinsHash;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class GraphClt {

    public TGraphFSServer.Client[] conns;
    public TGraphFSServer.AsyncClient[] asyncClients;

    public ArrayList<String> allSrvs;
    public int port;
    public int serverNum;

    public GraphClt(int port, ArrayList<String> alls) {
        this.allSrvs = alls;
        this.port = port;
        this.serverNum = allSrvs.size();
        this.conns = new TGraphFSServer.Client[this.serverNum];
        this.asyncClients = new TGraphFSServer.AsyncClient[this.serverNum];

        for (int i = 0; i < this.serverNum; i++) {
            String addrPort = this.allSrvs.get(i);
            String addr = addrPort.split(":")[0];
            this.port = Integer.parseInt(addrPort.split(":")[1]);

            //TTransport transport = new TSocket(addr, port);
            TTransport transport = new TFramedTransport(new TSocket(addr, this.port));
            try {
                transport.open();
            } catch (TTransportException e) {
                GLogger.error("FATAL ERROR: Client can not connect to %s:%s", addr, this.port);
                System.exit(0);
            }
            TProtocol protocol = new TBinaryProtocol(transport);
            TGraphFSServer.Client client = new TGraphFSServer.Client(protocol);
            conns[i] = client;
            try {
                asyncClients[i] = new TGraphFSServer.AsyncClient(new TBinaryProtocol.Factory(),
						new TAsyncClientManager(),
						new TNonblockingSocket(addr, this.port));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public TGraphFSServer.Client getClientConn(int target) throws TTransportException {
        return conns[target];
    }

    public synchronized TGraphFSServer.AsyncClient getAsyncClientConn(int target) throws TTransportException{
        return asyncClients[target];
    }

    abstract public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException;

    abstract public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException;

    abstract public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException;


    public int submitTravel(List<SingleStep> travelPlan) throws TException {
        long ts = System.currentTimeMillis();
        return submitTravel(travelPlan, ts);
    }

    public int submitTravel(List<SingleStep> travelPlan, long ts) throws TException {
        TravelCommand tc = new TravelCommand();
        long tid = System.currentTimeMillis();

        JSONArray array = new JSONArray();
        for (SingleStep ss : travelPlan) {
            JSONObject jo = new JSONObject();
            jo.put("value", ss.genJSON());
            array.add(jo);
        }
        JSONCommand jc = new JSONCommand();
        jc.add("travel_payload", array);

        tc.setType(TravelCommandType.TRAVEL_MASTER)
                .setTravelId(0L).setStepId(0).setReply_to(0)
                .setTs(ts).setPayload(jc.genString());
        int serverId = Math.abs((int) tid) % this.serverNum;
        getClientConn(serverId).syncTravelMaster(tc);
        return 0;
    }

    public int submitSyncTravel(List<SingleStep> travelPlan) throws TException {
        long ts = System.currentTimeMillis();
        return submitSyncTravel(travelPlan, ts);
    }

    public int submitSyncTravel(List<SingleStep> travelPlan, long ts) throws TException {
        TravelCommand tc = new TravelCommand();
        long tid = System.currentTimeMillis();

        JSONArray array = new JSONArray();
        for (SingleStep ss : travelPlan) {
            JSONObject jo = new JSONObject();
            jo.put("value", ss.genJSON());
            array.add(jo);
        }
        JSONCommand jc = new JSONCommand();
        jc.add("travel_payload", array);

        tc.setType(TravelCommandType.SYNC_TRAVEL_MASTER)
                .setTravelId(0L).setStepId(0).setReply_to(0)
                .setTs(ts).setPayload(jc.genString());
        int serverId = Math.abs((int) tid) % this.serverNum;
        getClientConn(serverId).syncTravelMaster(tc);
        return 0;
    }

    protected int getEdgeLocation(byte[] src, int serverNum) {
        JenkinsHash jh = new JenkinsHash();
        int hashi = Math.abs(jh.hash32(src));
        return (hashi % serverNum);
    }

}
