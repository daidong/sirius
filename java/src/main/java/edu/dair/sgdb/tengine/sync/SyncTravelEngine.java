package edu.dair.sgdb.tengine.sync;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.tengine.TravelLocalReader;
import edu.dair.sgdb.tengine.prefetch.PreLoadMemoryPool;
import edu.dair.sgdb.tengine.travel.JSONCommand;
import edu.dair.sgdb.tengine.travel.SingleRestriction;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.thrift.TravelCommand;
import edu.dair.sgdb.thrift.TravelCommandType;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SyncTravelEngine {

    private AbstractSrv instance;
    public ConcurrentHashMap<Long, SyncTravelStatus> travel_status;
    public HashMap<Long, byte[]> tsrcs;
    public PreLoadMemoryPool pool;

    public SyncTravelEngine(AbstractSrv s) {
        this.instance = s;
        this.travel_status = new ConcurrentHashMap<>();
        this.tsrcs = new HashMap<>();
        this.pool = new PreLoadMemoryPool();
    }

    public boolean is_travel_step_started(long travelId, int stepId) {
        return this.travel_status.get(travelId).is_step_started(stepId);
    }

    public void set_current_step(long travelId, int stepId) {
        this.travel_status.get(travelId).set_current_step(stepId);
    }

    public HashSet<ByteBuffer> get_vertex_of_step_in_travel(long travelId, int stepId) {
        return this.travel_status.get(travelId).get_vertex_of_step(stepId);
    }

    // Sync Server Operations
    private void add_servers_to_sync_of_step(long travelId, int stepId, int src, Set<Integer> sset, int type) {
        this.travel_status.get(travelId).add_servers_to_sync_of_step(stepId, src, sset, type);
    }

    private void remove_server_to_sync_of_step(long travelId, int stepId, int src, int dst, int type) {
        this.travel_status.get(travelId).remove_server_to_sync_of_step(stepId, src, dst, type);
    }

    private boolean is_sync_servers_empty(long travelId, int stepId) {
        return this.travel_status.get(travelId).is_servers_to_sync_empty(stepId);
    }

    private HashSet<SyncTravelStatus.SyncServerPair> get_sync_servers(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).get_servers_to_sync_of_step(stepId);
    }

    // Sync Results Operations
    private void add_to_travel_results(long travelId, List<KeyValue> vals) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return;
        }
        this.travel_status.get(travelId).add_to_travel_results(vals);
    }

    private void record_sync_travel_start_time(long travelId) {
        this.travel_status.get(travelId).sync_travel_master_start_time = System.currentTimeMillis();
    }

    // Sync Time Operations
    private long get_sync_travel_start_time(long travelId) {
        return this.travel_status.get(travelId).sync_travel_master_start_time;
    }

    private void set_sync_travel_plan(long travelId, ArrayList<SingleStep> plan) {
        this.travel_status.putIfAbsent(travelId, new SyncTravelStatus(travelId));
        this.travel_status.get(travelId).set_travel_plan(plan);
    }

    public ArrayList<SingleStep> get_sync_travel_plan(long travelId) {
        return this.travel_status.get(travelId).get_travel_plan();
    }

    // Sync Statistic Operation
    public void incrEdge2DstLocalCounter(long travelId, int s) {
        this.travel_status.putIfAbsent(travelId, new SyncTravelStatus(travelId));
        this.travel_status.get(travelId).incrEdge2DstLocalCounter(s);
    }

    public long getEdge2DstLocalCounter(long travelId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return 0L;
        }
        return this.travel_status.get(travelId).getEdge2DstLocalCounter();
    }

    private HashMap<Integer, HashSet<ByteBuffer>> get_servers_from_keys(List<byte[]> keySet) {
        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
        for (byte[] key : keySet) {
            Set<Integer> servers = instance.getVertexLoc(key);
            for (int s : servers) {
                if (!perServerVertices.containsKey(s)) {
                    perServerVertices.put(s, new HashSet<ByteBuffer>());
                }
                perServerVertices.get(s).add(ByteBuffer.wrap(key));
            }
        }
        return perServerVertices;
    }

    public String gen_json_string_from_travel_plan(ArrayList<SingleStep> travelPlan) {
        JSONArray array = new JSONArray();
        for (SingleStep ss : travelPlan) {
            JSONObject jo = new JSONObject();
            jo.put("value", ss.genJSON());
            array.add(jo);
        }
        JSONCommand jc = new JSONCommand();
        jc.add("travel_payload", array);
        return jc.genString();
    }

    public ArrayList<SingleStep> build_travel_plan_from_json_string(String payloadString) {
        JSONCommand js = new JSONCommand();
        Map request = js.parse(payloadString);
        JSONArray payload = (JSONArray) request.get("travel_payload");
        ArrayList<SingleStep> travelPlan = new ArrayList<>();
        for (int i = 0; i < payload.size(); i++) {
            JSONObject idx = (JSONObject) payload.get(i);
            JSONObject obj = (JSONObject) idx.get("value");
            SingleStep ss = SingleStep.parseJSON(obj.toString());
            travelPlan.add(ss);
        }
        return travelPlan;
    }

    private int rpc_req_sync_travel(HashSet<ByteBuffer> keys_set,
                                    ArrayList<SingleStep> travelPlan,
                                    long travelId,
                                    int current_step,
                                    int replyTo,
                                    int getFrom,
                                    int localId,
                                    int sub_type,
                                    long ts,
                                    int s) throws TException {

        List<byte[]> nextKeys = new ArrayList<byte[]>();
        for (ByteBuffer bb : keys_set) {
            byte[] tbb = NIOHelper.getActiveArray(bb);
            nextKeys.add(tbb);
        }

        travelPlan.get(current_step).vertexKeyRestrict =
                new SingleRestriction.InWithValues("key".getBytes(), nextKeys);
        String travelPayLoad = gen_json_string_from_travel_plan(travelPlan);

        TravelCommand tc1 = new TravelCommand();
        tc1.setType(TravelCommandType.SYNC_TRAVEL)
                .setTravelId(travelId)
                .setStepId(current_step)
                .setReply_to(replyTo)
                .setGet_from(getFrom)
                .setLocal_id(localId)
                .setTs(ts)
                .setPayload(travelPayLoad)
                .setSub_type(sub_type);

        GLogger.info("\t Travel master [%d] sends travel command to %d at [%lld]",
                instance.getLocalIdx(), s, System.nanoTime());

        TGraphFSServer.Client client = instance.getClientConn(s);
        int rtn = client.syncTravel(tc1);
        instance.releaseClientConn(s, client);
        return rtn;
    }

    private int rpc_req_start_next_sync_travel(long travelId,
                                               int current_step,
                                               int replyTo,
                                               int getFrom,
                                               long ts,
                                               int s) throws TException {
        // broadcast SYNC_TRAVEL_START to start next round of synchronous traversal.
        TravelCommand tc1 = new TravelCommand();
        tc1.setType(TravelCommandType.SYNC_TRAVEL_START)
                .setTravelId(travelId)
                .setStepId(current_step)
                .setReply_to(replyTo)
                .setGet_from(getFrom)
                .setTs(ts);

        GLogger.info("\t Travel master [%d] sends TravelStart to [%d] at [%lld]",
                instance.getLocalIdx(), s, System.nanoTime());

        TGraphFSServer.Client client = instance.getClientConn(s);
        int rtn = client.syncTravelStart(tc1);
        instance.releaseClientConn(s, client);
        return rtn;
    }

    private int rpc_req_sync_travel_rtn(ArrayList<KeyValue> vals,
                                        long travelId,
                                        int current_step,
                                        int reply_to,
                                        int getFrom,
                                        int localId,
                                        long ts,
                                        int s) throws TException {




        TravelCommand tc = new TravelCommand();
        tc.setType(TravelCommandType.SYNC_TRAVEL_RTN).setTravelId(travelId)
                .setStepId(current_step).setGet_from(getFrom).setLocal_id(instance.getLocalIdx());
        tc.setVals(vals);

        TGraphFSServer.Client client = instance.getClientConn(s);
        GLogger.info("%d Send TravelRtn to %d at %d",
                instance.getLocalIdx(), s, System.nanoTime());
        int rtn = client.syncTravelRtn(tc);
        instance.releaseClientConn(s, client);
        return rtn;
    }

    private int rpc_req_sync_travel_extend(List<Integer> addrs,
                                           long travelId,
                                           int stepId,
                                           int reply_to,
                                           int get_from,
                                           int local_id,
                                           int sub_type,
                                           long ts,
                                           int s) throws TException {

        TravelCommand tc_ext = new TravelCommand();
        tc_ext.setTravelId(travelId)
                .setStepId(stepId).setType(TravelCommandType.SYNC_TRAVEL_EXTEND)
                .setGet_from(get_from)
                .setReply_to(reply_to)
                .setExt_srv(addrs)
                .setLocal_id(local_id)
                .setSub_type(sub_type);


        TGraphFSServer.Client client = instance.getClientConn(s);
        GLogger.info("%d Send TraveExtend to %d including %s at %d",
                    instance.getLocalIdx(), s, addrs, System.nanoTime());
        int rtn = client.syncTravelExtend(tc_ext);
        instance.releaseClientConn(s, client);
        return rtn;
    }

    private int rpc_req_sync_travel_finish(long travelId,
                                           int stepId,
                                           int replyTo,
                                           int getFrom,
                                           int localId,
                                           int sub_type,
                                           long ts,
                                           int s) throws TException {

        TravelCommand tc3 = new TravelCommand();
        tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH).setTravelId(travelId)
                .setStepId(stepId).setGet_from(getFrom).setLocal_id(localId)
                .setReply_to(replyTo).setTs(ts).setSub_type(sub_type); //finish on edges

        //GLogger.warn("[%d] SyncTravelEdgeWorker stepId[%d] Finish %d to %s (%d)",
        //        inst.getLocalIdx(), (stepId), getFrom, inst.getLocalIdx(), 1);

        TGraphFSServer.Client client1 = instance.getClientConn(s);
        GLogger.info("\t\t\t %d Send TravelFinish to %d at %d on reading edges",
                instance.getLocalIdx(), s, System.nanoTime());
        int rtn = client1.syncTravelFinish(tc3);
        instance.releaseClientConn(s, client1);
        return rtn;
    }
    /**
     * Sync Engine Codes
     */
    public int syncTravelMaster(TravelCommand tc) throws TException {
        int current_step, replyTo;
        long ts = tc.getTs();
        long travelId = ts + ((long) (instance.getLocalIdx() + 1) * (1L << 32));

        // Create a Travel Status for current traversal
        SyncTravelStatus current_travel_status = new SyncTravelStatus(travelId);
        this.travel_status.putIfAbsent(travelId, current_travel_status);
        record_sync_travel_start_time(travelId);

        GLogger.info("Server [%d] Recieve Sync Travel Request From client at time [%lld]",
                instance.getLocalIdx(), System.nanoTime());

        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(tc.getPayload());
        set_sync_travel_plan(travelId, travelPlan);

        current_step = 0;
        replyTo = instance.getLocalIdx();

        // This assume the starting point of any graph traversal only has one key.
        List<byte[]> keySet = travelPlan.get(current_step).vertexKeyRestrict.values();
        for (byte[] t : keySet) {
            this.tsrcs.put(travelId, t);
        }

        //register the servers for checking before starting next step
        HashMap<Integer, HashSet<ByteBuffer>> servers_store_the_keys = get_servers_from_keys(keySet);
        add_servers_to_sync_of_step(travelId, current_step, replyTo, servers_store_the_keys.keySet(), 0);

        set_current_step(travelId, current_step);

        // these servers will cache current step and wait for the SYNC_TRAVEL_START command;
        for (int s : servers_store_the_keys.keySet()) {
            HashSet<ByteBuffer> keys_set = servers_store_the_keys.get(s);
            rpc_req_sync_travel(keys_set, travelPlan, travelId, current_step, replyTo,
                    instance.getLocalIdx(), instance.getLocalIdx(), 0, ts, s);
        }

        // start the travel step 0
        for (int s : servers_store_the_keys.keySet()) {
            rpc_req_start_next_sync_travel(travelId, current_step, replyTo, instance.getLocalIdx(), ts, s);
        }

        //wait for all travel to finish
        current_travel_status.aLock.lock();
        try {
            while (current_travel_status.finished == false){
                try {
                    current_travel_status.condVar.await();
                } catch (InterruptedException e) {
                }
            }
            current_travel_status.finished = true;
            current_travel_status.sync_travel_master_stop_time = System.currentTimeMillis();
        } finally {
            current_travel_status.aLock.unlock();
        }

        return 0;
    }

    public int syncTravel(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int current_step = tc.getStepId();
        int subType = tc.getSub_type();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();

        GLogger.info("\t\t Server [%d] receives syncTravel command type: %d from [%d] at [%lld]",
                instance.getLocalIdx(), subType, getFrom, System.nanoTime());

        String payloadString = tc.getPayload();
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payloadString);
        set_sync_travel_plan(travelId, travelPlan);

        SingleStep currStep = travelPlan.get(current_step);
        HashSet<ByteBuffer> kSets = new HashSet<ByteBuffer>();
        for (byte[] key : currStep.vertexKeyRestrict.values()) {
            kSets.add(ByteBuffer.wrap(key));
        }

        // There are two types of SYNC_TRAVEL commands.
        // One for vertex processing; one for edge processing;
        if (subType == 0) {
            this.travel_status.get(travelId).add_vertex_to_current_step(current_step, kSets);

        } else {
            Runnable edge_worker = new SyncTravelEdgeWorker(instance,this,travelId,
                                                            current_step,getFrom,replyTo,ts,kSets);
            instance.workerPool.execute(edge_worker);
        }
        return 0;
    }

    public int syncTravelStart(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int current_step = tc.getStepId();
        int subType = tc.getSub_type();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();

        set_current_step(travelId, current_step);

        instance.workerPool.execute(
                new SyncTravelVertexWorker(instance,this,travelId,current_step,replyTo,getFrom,ts)
        );

        GLogger.info("\t\t Server [%d] receives TravelStart from [%d] at [%lld]",
                instance.getLocalIdx(),
                tc.getGet_from(), System.nanoTime());

        return 0;
    }

    public int deleteSyncTravelInstance(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        travel_status.remove(travelId);
        return 0;
    }

    public int syncTravelRtn(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int srcServer = tc.getGet_from();
        int dstServer = tc.getLocal_id();

        GLogger.info("\t\t Server [%d] receives TravelReturn from [%d] at [%lld]",
                instance.getLocalIdx(), dstServer, System.nanoTime());

        for (int i = 0; i < instance.serverNum; i++) {
            remove_server_to_sync_of_step(travelId, stepId, i, dstServer, 0);
        }

        //remove_server_to_sync_of_step(travelId, stepId, srcServer, dstServer, 0);
        if (tc.isSetVals()) {
            List<KeyValue> vals = tc.getVals();
            add_to_travel_results(travelId, vals);
        }

        long costTime = System.currentTimeMillis() - get_sync_travel_start_time(travelId);

        if (is_sync_servers_empty(travelId, stepId)) {
            GLogger.info("in SyncTravelRtn, Step %d Finishes at %d", stepId, costTime);
            GLogger.info("Server [%d] TravelId[%d] Starting %s, costs: %d",
                    instance.getLocalIdx(), travelId,
                    new String(this.tsrcs.get(travelId)),
                    costTime);
        }

        //signal that all travels finished
        SyncTravelStatus current_travel_status = this.travel_status.get(travelId);
        current_travel_status.aLock.lock();
        try {
            current_travel_status.finished = true;
            current_travel_status.condVar.signal();
        } finally {
            current_travel_status.aLock.unlock();
        }
        return 0;
    }

    public int syncTravelExtend(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int getFrom = tc.getGet_from();
        int subType = tc.getSub_type();
        int remote = tc.getLocal_id();

        GLogger.info("\t\t Server [%d] receives TravelExtend from [%d] at [%lld]",
                instance.getLocalIdx(), remote, System.nanoTime());

        HashSet<Integer> srvs = new HashSet<Integer>(tc.getExt_srv());
        add_servers_to_sync_of_step(travelId, stepId, getFrom, srvs, subType);

        GLogger.debug("[%d] stepId[%d] Extends %d -> %s",
                instance.getLocalIdx(), stepId, getFrom, srvs);
        return 0;
    }

    public int syncTravelFinish(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int src = tc.getGet_from();
        int dst = tc.getLocal_id();
        int type = tc.getSub_type();

        int replyTo = tc.getReply_to();
        long ts = tc.getTs();

        //dst or local_id is the sender. Not src.
        GLogger.info("\t\t Server [%d] receives TravelFinish from [%d] at [%lld]",
                instance.getLocalIdx(), dst, System.nanoTime());

        if (type == 0) {
            //vertex worker finish. should not start next step.
            for (int i = 0; i < instance.serverNum; i++) {
                remove_server_to_sync_of_step(travelId, stepId, i, dst, type);
            }

            //GLogger.warn("[%d] stepId[%d] sync servers: %s",
            //        inst.getLocalIdx(), stepId, get_servers_to_sync_of_step(travelId, stepId));

        } else {
            //GLogger.warn("[%d] stepId[%d] receives %d -> %d (%d)",
            //        inst.getLocalIdx(), stepId, src, dst, type);

            remove_server_to_sync_of_step(travelId, stepId, src, dst, type);

            //GLogger.warn("[%d] stepId[%d] sync servers: %s",
            //        inst.getLocalIdx(), stepId, get_servers_to_sync_of_step(travelId, stepId));

            long costTime = System.currentTimeMillis() - get_sync_travel_start_time(travelId);

            if (is_sync_servers_empty(travelId, stepId) && !is_sync_servers_empty(travelId, stepId + 1)) {
                GLogger.info("\t\t Server [%d] TravelFinish Step %d Finishes at %d",
                        instance.getLocalIdx(), stepId, costTime);

                HashSet<SyncTravelStatus.SyncServerPair> addrs = get_sync_servers(travelId, stepId + 1);
                HashSet<Integer> servers = new HashSet<>();
                for (SyncTravelStatus.SyncServerPair pair : addrs) {
                    servers.add(pair.endTo);
                }

                for (int s : servers) {
                    GLogger.info("%d Send TravelStart to %d at %d for %d",
                            instance.getLocalIdx(), s, System.nanoTime(), stepId + 1);

                    rpc_req_start_next_sync_travel(travelId, stepId+1, replyTo, instance.getLocalIdx(), ts, s);
                }

            } else if (is_sync_servers_empty(travelId, stepId) && is_sync_servers_empty(travelId, stepId + 1)) {

                GLogger.info("in SyncTravelRtn, Step %d Finishes at %d", stepId, costTime);
                GLogger.info("[%d] TravelId[%d] Starting %s, costs: %d",
                        instance.getLocalIdx(), travelId,
                        new String(this.tsrcs.get(travelId)),
                        costTime);

                //signal that all travels finished
                SyncTravelStatus current_travel_status = this.travel_status.get(travelId);
                current_travel_status.aLock.lock();
                try {
                    current_travel_status.finished = true;
                    current_travel_status.condVar.signal();
                } finally {
                    current_travel_status.aLock.unlock();
                }
            }
        }

        return 0;
    }


    private class SyncTravelVertexWorker implements Runnable {

        AbstractSrv instance;
        SyncTravelEngine engine;
        long travelId = 0;
        int stepId = 0;
        int replyTo = 0;
        int getFrom = 0;
        long ts = 0L;
        Set<Integer> targets = null;

        public SyncTravelVertexWorker(AbstractSrv s, SyncTravelEngine e, long tid, int sid,
                                      int replyTo, int getFrom,
                                      long ts) {
            this.instance = s;
            this.engine = e;
            this.travelId = tid;
            this.stepId = sid;
            this.replyTo = replyTo;
            this.getFrom = getFrom;
            this.ts = ts;
        }

        @Override
        public void run() {
            boolean lastStep = false;
            ArrayList<SingleStep> travelPlan = engine.get_sync_travel_plan(travelId);
            SingleStep currStep = travelPlan.get(stepId);
            if (stepId == (travelPlan.size() - 1)) {
                lastStep = true;
            }

            GLogger.info("\t\t\t Server %d Read Local Vertices for %d at %d",
                    instance.getLocalIdx(), stepId, System.nanoTime());

            ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(
                    this.instance.localStore,
                    engine.get_vertex_of_step_in_travel(this.travelId, this.stepId),
                    currStep, ts);

            GLogger.info("\t\t\t Server %d Finish Read %d Local Vertices for %d at %d",
                    instance.getLocalIdx(), passedVertices.size(), stepId, System.nanoTime());

            if (lastStep) {

                ArrayList<KeyValue> vals = new ArrayList<>();
                for (byte[] vertex : passedVertices) {
                    KeyValue kv = new KeyValue();
                    kv.setKey(vertex);
                    vals.add(kv);
                }

                try {
                    rpc_req_sync_travel_rtn(vals, travelId, stepId, -1, getFrom, instance.getLocalIdx(), ts, replyTo);
                } catch (TException e) {
                    e.printStackTrace();
                }

            } else {

                HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
                for (byte[] key : passedVertices) {
                    Set<Integer> servers = instance.getEdgeLocs(key, EdgeType.OUT.get());
                    for (Integer s : servers) {
                        if (!perServerVertices.containsKey(s)) {
                            perServerVertices.put(s, new HashSet<ByteBuffer>());
                        }
                        perServerVertices.get(s).add(ByteBuffer.wrap(key));
                    }
                }

                // report to coordinator, round 'stepId' should add these
                // many extra servers besides vertices locations
                List<Integer> addrs = new ArrayList<Integer>(perServerVertices.keySet());

                try {
                    rpc_req_sync_travel_extend(addrs, travelId, stepId, -1,
                                                instance.getLocalIdx(), instance.getLocalIdx(),
                                                1, 0, replyTo);
                } catch (TException e) {
                    e.printStackTrace();
                }

                //send travel commands to all servers with the edges
                for (int s : perServerVertices.keySet()) {
                    HashSet<ByteBuffer> keys_set = perServerVertices.get(s);
                    try {
                        rpc_req_sync_travel(keys_set, travelPlan, travelId, stepId, replyTo,
                                instance.getLocalIdx(), s, 1, ts, s);
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }

                //finish all sends, send SYNC_TRAVEL_FINISH Command
                try {
                    rpc_req_sync_travel_finish(travelId,stepId,replyTo,getFrom,
                                            instance.getLocalIdx(),0,ts,replyTo);
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }
    }


    private class SyncTravelEdgeWorker implements Runnable {

        AbstractSrv instance;
        SyncTravelEngine engine;
        long travelId = 0;
        int stepId = 0;
        int getFrom = 0;
        int replyTo = 0;
        long ts = 0;
        HashSet<ByteBuffer> keys;
        Set<Integer> targets = null;

        public SyncTravelEdgeWorker(AbstractSrv s,
                                    SyncTravelEngine e,
                                    long tid,
                                    int sid,
                                    int getFrom,
                                    int replyTo,
                                    long ts,
                                    HashSet<ByteBuffer> kSets) {
            this.instance = s;
            this.engine = e;
            this.travelId = tid;
            this.stepId = sid;
            this.getFrom = getFrom;
            this.replyTo = replyTo;
            this.ts = ts;
            this.keys = kSets;
        }

        @Override
        public void run() {
            GLogger.info("\t\t\t Server [%d] Start Read Edges for stepId [%d], Command from %d",
                    instance.getLocalIdx(), stepId, getFrom);

            ArrayList<SingleStep> travelPlan = engine.get_sync_travel_plan(travelId);
            SingleStep currStep = travelPlan.get(stepId);

            ArrayList<byte[]> passedVertices = new ArrayList<>();
            for (ByteBuffer k : keys) {
                byte[] tk = NIOHelper.getActiveArray(k);
                passedVertices.add(tk);
            }
            HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();

            GLogger.info("\t\t\t Server [%d] Read Local Edges for %d at %d",
                    instance.getLocalIdx(), stepId, System.nanoTime());

            HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(
                    this.instance.localStore, passedVertices, currStep, ts);

            GLogger.info("\t\t\t Server [%d] Finish Read %d Local Edges for step %d at %d",
                    instance.getLocalIdx(), nextVertices.size(), stepId, System.nanoTime());

            for (byte[] v : nextVertices) {
                Set<Integer> servers = instance.getVertexLoc(v);
                for (Integer s : servers) {
                    if (!perServerVertices.containsKey(s)) {
                        perServerVertices.put(s, new HashSet<ByteBuffer>());
                    }
                    perServerVertices.get(s).add(ByteBuffer.wrap(v));
                }
            }

            // report to coordinator, round 'stepId + 1' should have these many servers, and they are:
            List<Integer> addrs = new ArrayList<Integer>(perServerVertices.keySet());
            try {
                rpc_req_sync_travel_extend(addrs, travelId, stepId + 1, -1, instance.getLocalIdx(),
                        instance.getLocalIdx(), 0, 0, replyTo);
            } catch (TException e) {
                e.printStackTrace();
            }


            // send sync_travel command to all covered servers
            this.targets = new HashSet<>(perServerVertices.keySet());

            for (int s : perServerVertices.keySet()) {
                HashSet<ByteBuffer> keys_set = perServerVertices.get(s);
                int current_step = stepId + 1;
                try {
                    rpc_req_sync_travel(keys_set, travelPlan, travelId, current_step, replyTo,
                            instance.getLocalIdx(), instance.getLocalIdx(), 0, ts, s);
                } catch (TException e) {
                    e.printStackTrace();
                }

            }

            try {
                rpc_req_sync_travel_finish(travelId, stepId, replyTo, getFrom,
                        instance.getLocalIdx(),1, ts, replyTo);
            } catch (TException e) {
                e.printStackTrace();
            }


            if (perServerVertices.keySet().isEmpty()) {
                try {
                    rpc_req_sync_travel_finish(travelId,stepId,replyTo, getFrom,instance.getLocalIdx(),1,ts,replyTo);
                } catch (TException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
