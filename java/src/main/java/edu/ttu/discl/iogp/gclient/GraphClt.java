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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class GraphClt {

    public TGraphFSServer.Client[] conns;
    public ArrayList<String> allSrvs;
    public int port;
    public int serverNum;

    public GraphClt(int port, ArrayList<String> alls) {
        this.allSrvs = alls;
        this.port = port;
        this.serverNum = allSrvs.size();
        this.conns = new TGraphFSServer.Client[this.serverNum];
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
            }
            TProtocol protocol = new TBinaryProtocol(transport);
            TGraphFSServer.Client client = new TGraphFSServer.Client(protocol);
            conns[i] = client;
        }

    }

    public TGraphFSServer.Client getClientConn(int target) throws TTransportException {
        return conns[target];
    }

    abstract public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException;

    abstract public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, long ts) throws TException;

    abstract public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException;

    abstract public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value, long ts) throws TException;

    abstract public int batch_insert(Batch ib);

    abstract public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException;

    abstract public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType, long ts) throws TException;

    abstract public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType, long start_ts, long end_ts) throws TException;

    abstract public HashMap<Integer, Integer> getStats();
    
    public void EchoTest(){
        ByteBuffer payload = ByteBuffer.wrap("Hello World".getBytes());
        for (int target = 0; target < this.serverNum; target++){
            try {
                int t = this.conns[target].echo(target, payload);
                if (t == 0)
                    System.out.println("Server " + target + " reply OK");
            } catch (TException ex) {
                Logger.getLogger(GraphClt.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

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

        tc.setType(TravelCommandType.TRAVEL_MASTER).setTravelId(0L).setStepId(0).setReply_to(0).setTs(ts).setPayload(jc.genString());
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

        tc.setType(TravelCommandType.SYNC_TRAVEL_MASTER).setTravelId(0L).setStepId(0).setReply_to(0).setTs(ts).setPayload(jc.genString());
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
