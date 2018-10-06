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
import java.util.concurrent.atomic.AtomicInteger;

public class SyncTravelEngine {

    private AbstractSrv instance;
    public ConcurrentHashMap<Long, SyncTravelStatus> travel_status;
    public HashMap<Long, byte[]> tsrcs;
    public AtomicInteger mid;
    public PreLoadMemoryPool pool;

    public SyncTravelEngine(AbstractSrv s) {
        this.instance = s;
        this.travel_status = new ConcurrentHashMap<>();
        this.tsrcs = new HashMap<>();
        this.mid = new AtomicInteger(1);
        this.pool = new PreLoadMemoryPool();
    }

    public boolean isStepStarted(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return false;
        }
        return this.travel_status.get(travelId).is_step_started(stepId);
    }

    public void set_current_step(long travelId, int stepId) {
        this.travel_status.get(travelId).set_current_step(stepId);
    }

    public HashSet<ByteBuffer> getSyncTravelVertices(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).get_vertex_of_step(stepId);
    }

    // Sync Server Operations
    private void add_servers_to_sync_of_step(long travelId, int stepId, int src, Set<Integer> sset, int type) {
        this.travel_status.get(travelId).add_servers_to_sync_of_step(stepId, src, sset, type);
    }

    private void remove_server_to_sync_of_step(long travelId, int stepId, int src, int dst, int type) {
        this.travel_status.get(travelId).remove_server_to_sync_of_step(stepId, src, dst, type);
    }

    private boolean isSyncServerEmpty(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return false;
        }
        return this.travel_status.get(travelId).is_servers_to_sync_empty(stepId);
    }

    private HashSet<SyncTravelStatus.SyncServerPair> getSyncServers(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).get_servers_to_sync_of_step(stepId);
    }

    // Sync Results Operations
    private void addToTravelResults(long travelId, List<KeyValue> vals) {
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

    private HashMap<Integer, HashSet<ByteBuffer>> getVertexBroadcastServers(List<byte[]> keySet) {
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

    /**
     * Sync Engine Codes
     */
    public int syncTravelMaster(TravelCommand tc) throws TException {
        int current_step, replyTo;
        long ts = tc.getTs();
        long travelId = (long) this.mid.incrementAndGet()
                         + ((long) (instance.getLocalIdx() + 1) * (1L << 32));

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

        List<byte[]> keySet = travelPlan.get(current_step).vertexKeyRestrict.values();
        GLogger.debug("Server [%d] Receives Travel %d on %s at %d",
                instance.getLocalIdx(), travelId,
                new String(keySet.get(0)),
                get_sync_travel_start_time(travelId));

        // This assume the starting point of any graph traversal only has one key.
        for (byte[] t : keySet) {
            this.tsrcs.put(travelId, t);
        }

        HashMap<Integer, HashSet<ByteBuffer>> servers_store_the_keys = getVertexBroadcastServers(keySet);

        //register the execution
        //we are extending vertices
        add_servers_to_sync_of_step(travelId, current_step, replyTo, servers_store_the_keys.keySet(), 0);

        // broadcast vertices to servers;
        // these servers will cache current step and wait for the SYNC_TRAVEL_START command;
        for (int s : servers_store_the_keys.keySet()) {
            List<byte[]> nextKeys = new ArrayList<byte[]>();
            for (ByteBuffer bb : servers_store_the_keys.get(s)) {
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
                    .setGet_from(instance.getLocalIdx())
                    .setLocal_id(instance.getLocalIdx())
                    .setTs(ts)
                    .setPayload(travelPayLoad)
                    .setSub_type(0);

            GLogger.info("\t Travel master [%d] sends travel command to %d at [%lld]",
                    instance.getLocalIdx(), s, System.nanoTime());

            TGraphFSServer.Client client = instance.getClientConn(s);
            client.syncTravel(tc1);
            instance.releaseClientConn(s, client);
        }


        set_current_step(travelId, current_step);

        // broadcast SYNC_TRAVEL_START to start next round of synchronous traversal.
        TravelCommand tc1 = new TravelCommand();
        tc1.setType(TravelCommandType.SYNC_TRAVEL_START)
                .setTravelId(travelId)
                .setStepId(current_step)
                .setReply_to(replyTo)
                .setGet_from(instance.getLocalIdx())
                .setTs(ts);

        for (int s : servers_store_the_keys.keySet()) {
            GLogger.info("\t Travel master [%d] sends TravelStart to [%d] at [%lld]",
                    instance.getLocalIdx(), s, System.nanoTime());

            TGraphFSServer.Client client = instance.getClientConn(s);
            client.syncTravelStart(tc1);
            instance.releaseClientConn(s, client);
        }

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

    public synchronized int syncTravel(TravelCommand tc) throws TException {
        UUID id = UUID.randomUUID();
        long travelId = tc.getTravelId();
        int current_step = tc.getStepId();
        int subType = tc.getSub_type();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();
        GLogger.debug("[%d] [travel] [%s] [Step] [%d] Enter",
                instance.getLocalIdx(), id.toString(), current_step);

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
            instance.workerPool.execute(
                    new SyncTravelEdgeWorker(
                            instance,
                            this,
                            travelId,
                            current_step,
                            getFrom,
                            replyTo,
                            ts,
                            kSets
                    )
            );
        }

        GLogger.debug("[%d] [travel] [%s] [Step] [%d] Finish",
                instance.getLocalIdx(), id.toString(), current_step);
        return 0;
    }

    public synchronized int syncTravelStart(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int current_step = tc.getStepId();
        int subType = tc.getSub_type();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();

        set_current_step(travelId, current_step);

        boolean lastStep = false;
        ArrayList<SingleStep> travelPlan = this.get_sync_travel_plan(travelId);
        SingleStep currStep = travelPlan.get(current_step);
        if (current_step == (travelPlan.size() - 1)) {
            lastStep = true;
        }

        int tid = this.mid.addAndGet(1);
        GLogger.info("\t\t Server [%d] read local vertices for step [%d] at [%lld]",
                instance.getLocalIdx(), current_step, System.nanoTime());

        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(
                this.instance.localStore,
                this.getSyncTravelVertices(travelId, current_step),
                currStep, ts);

        GLogger.info("\t\t Server [%d] reads %d local vertices for step [%d] at [%lld]",
                instance.getLocalIdx(), passedVertices.size(), current_step, System.nanoTime());

        if (lastStep) {
            TravelCommand tc1 = new TravelCommand();
            tc.setType(TravelCommandType.SYNC_TRAVEL_RTN).setTravelId(travelId)
                    .setStepId(current_step).setGet_from(getFrom).setLocal_id(instance.getLocalIdx());

            ArrayList<KeyValue> vals = new ArrayList<>();
            for (byte[] vertex : passedVertices) {
                KeyValue kv = new KeyValue();
                kv.setKey(vertex);
                vals.add(kv);
            }
            tc.setVals(vals);
            try {
                TGraphFSServer.Client client = instance.getClientConn(replyTo);
                GLogger.info("\t\t Server [%d] sends TravelRtn to [%d] at [%lld]",
                        instance.getLocalIdx(), replyTo, System.nanoTime());
                client.syncTravelRtn(tc1);
                instance.releaseClientConn(replyTo, client);
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

            List<Integer> addrs = new ArrayList<Integer>(perServerVertices.keySet());

            TravelCommand tc_ext = new TravelCommand();
            tc_ext.setTravelId(travelId)
                    .setStepId(current_step).setType(TravelCommandType.SYNC_TRAVEL_EXTEND)
                    .setGet_from(instance.getLocalIdx()).setExt_srv(addrs)
                    .setLocal_id(instance.getLocalIdx())
                    .setSub_type(1);

            try {
                TGraphFSServer.Client client = instance.getClientConn(replyTo);
                GLogger.info("\t\t Server [%d] sends TraveExtend to [%d] including [%s] at [%lld]",
                        instance.getLocalIdx(), replyTo, addrs, System.nanoTime());
                client.syncTravelExtend(tc_ext);
                instance.releaseClientConn(replyTo, client);
            } catch (TException e) {
                e.printStackTrace();
            }

            //send travel commands to all servers with the edges
            Set<Integer> targets = new HashSet<>(perServerVertices.keySet());
            for (int s : perServerVertices.keySet()) {
                List<byte[]> nextKeys = new ArrayList<>();
                for (ByteBuffer bb : perServerVertices.get(s)) {
                    byte[] tbb = NIOHelper.getActiveArray(bb);
                    nextKeys.add(tbb);
                }

                travelPlan.get(current_step).vertexKeyRestrict =
                        new SingleRestriction.InWithValues("key".getBytes(), nextKeys);
                String travelPayLoad = this.gen_json_string_from_travel_plan(travelPlan);

                TravelCommand tc1 = new TravelCommand();
                tc1.setType(TravelCommandType.SYNC_TRAVEL)
                        .setTravelId(travelId)
                        .setStepId(current_step)
                        .setReply_to(replyTo)
                        .setTs(ts)
                        .setPayload(travelPayLoad)
                        .setGet_from(instance.getLocalIdx())
                        .setLocal_id(s)
                        .setSub_type(1);

                try {
                    GLogger.info("\t\t Server [%d] sends SyncTravel to [%d] for the edges at [%lld]",
                            instance.getLocalIdx(), s, System.nanoTime());
                    TGraphFSServer.Client client = instance.getClientConn(s);
                    client.syncTravel(tc1);
                    instance.releaseClientConn(s, client);
                } catch (TException te) {
                }
            }

            TravelCommand tc3 = new TravelCommand();
            tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH)
                    .setTravelId(travelId)
                    .setStepId(current_step)
                    .setGet_from(getFrom)
                    .setLocal_id(instance.getLocalIdx())
                    .setReply_to(replyTo)
                    .setTs(ts)
                    .setSub_type(0); //finish on vertex

            try {
                TGraphFSServer.Client client = instance.getClientConn(replyTo);
                GLogger.info("%d Send TravelFinish to %d at %d finish on vertices",
                        instance.getLocalIdx(),
                        replyTo, System.nanoTime());
                client.syncTravelFinish(tc3);
                instance.releaseClientConn(replyTo, client);

            } catch (TException e) {
                e.printStackTrace();

            }
        }

        GLogger.info("\t\t Server [%d] receives TravelStart from [%d] at [%lld]",
                instance.getLocalIdx(),
                tc.getGet_from(), System.nanoTime());

        return 0;
    }

    public synchronized int deleteSyncTravelInstance(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        travel_status.remove(travelId);
        return 0;
    }

    public synchronized int syncTravelRtn(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int srcServer = tc.getGet_from();
        int dstServer = tc.getLocal_id();

        GLogger.info("%d Receive TravelReturn from %d at %d",
                instance.getLocalIdx(), dstServer, System.nanoTime());

        for (int i = 0; i < instance.serverNum; i++) {
            remove_server_to_sync_of_step(travelId, stepId, i, dstServer, 0);
        }
        //GLogger.warn("[%d] stepId[%d] sync servers: %s",
        //        inst.getLocalIdx(), stepId, get_servers_to_sync_of_step(travelId, stepId));

        //remove_server_to_sync_of_step(travelId, stepId, srcServer, dstServer, 0);
        if (tc.isSetVals()) {
            List<KeyValue> vals = tc.getVals();
            addToTravelResults(travelId, vals);
        }

        long costTime = System.currentTimeMillis() - get_sync_travel_start_time(travelId);


        if (isSyncServerEmpty(travelId, stepId)) {
            GLogger.info("in SyncTravelRtn, Step %d Finishes at %d", stepId, costTime);
            GLogger.info("[%d] TravelId[%d] Starting %s, costs: %d",
                    instance.getLocalIdx(), travelId,
                    new String(this.tsrcs.get(travelId)),
                    costTime);
            instance.workerPool.execute(new DeleteTravelInstance(instance, travelId));
        }

        Object lock = this.locks.get(travelId);
        synchronized (lock) {
            lock.notify();
        }

        return 0;
    }

    private class DeleteTravelInstance implements Runnable {

        AbstractSrv instance;
        long travelId;

        public DeleteTravelInstance(AbstractSrv instance, long travelId) {
            this.instance = instance;
            this.travelId = travelId;
        }

        @Override
        public void run() {
            TravelCommand tc = new TravelCommand();
            tc.setType(TravelCommandType.TRAVEL_SYNC_DEL).setTravelId(travelId);
            for (int s = 0; s < instance.serverNum; s++) {
                try {
                    if (s != instance.getLocalIdx()) {
                        TGraphFSServer.Client client = instance.getClientConn(s);
                        client.deleteSyncTravelInstance(tc);
                        instance.releaseClientConn(s, client);
                    } else {
                        deleteSyncTravelInstance(tc);
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized int syncTravelExtend(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int getFrom = tc.getGet_from();
        int subType = tc.getSub_type();
        int remote = tc.getLocal_id();

        GLogger.info("%d Receive TravelExtend from %d at %d",
                instance.getLocalIdx(), remote, System.nanoTime());

        HashSet<Integer> srvs = new HashSet<Integer>(tc.getExt_srv());
        add_servers_to_sync_of_step(travelId, stepId, getFrom, srvs, subType);

        GLogger.debug("[%d] stepId[%d] Extends %d -> %s",
                instance.getLocalIdx(), stepId, getFrom, srvs);
        return 0;
    }

    public synchronized int syncTravelFinish(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int src = tc.getGet_from();
        int dst = tc.getLocal_id();
        int type = tc.getSub_type();

        int replyTo = tc.getReply_to();
        long ts = tc.getTs();

        //dst or local_id is the sender. Not src.
        GLogger.info("%d Receive TravelFinish from %d at %d",
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

            if (isSyncServerEmpty(travelId, stepId) && !isSyncServerEmpty(travelId, stepId + 1)) {
                GLogger.warn("[%d] TravelFinish Step %d Finishes at %d",
                        instance.getLocalIdx(), stepId, costTime);

                /**
                 * Async Start
                 */
                HashSet<SyncTravelStatus.SyncServerPair> addrs = getSyncServers(travelId, stepId + 1);
                HashSet<Integer> servers = new HashSet<>();
                for (SyncTravelStatus.SyncServerPair pair : addrs) {
                    servers.add(pair.endTo);
                }

                TravelCommand tc1 = new TravelCommand();
                tc1.setType(TravelCommandType.SYNC_TRAVEL_START)
                        .setTravelId(travelId)
                        .setStepId(stepId + 1)
                        .setReply_to(replyTo)
                        .setGet_from(instance.getLocalIdx())
                        .setTs(ts);

                for (int s : servers) {
                    GLogger.info("%d Send TravelStart to %d at %d for %d",
                            instance.getLocalIdx(), s, System.nanoTime(), stepId + 1);

                    TGraphFSServer.Client client = instance.getClientConn(s);
                    client.syncTravelStart(tc1);
                    instance.releaseClientConn(s, client);

                }

            } else if (isSyncServerEmpty(travelId, stepId) && isSyncServerEmpty(travelId, stepId + 1)) {

                GLogger.info("in SyncTravelRtn, Step %d Finishes at %d", stepId, costTime);
                GLogger.info("[%d] TravelId[%d] Starting %s, costs: %d",
                        instance.getLocalIdx(), travelId,
                        new String(this.tsrcs.get(travelId)),
                        costTime);

                Object lock = this.locks.get(travelId);
                synchronized (lock) {
                    lock.notify();
                }
            }
        }

        return 0;
    }

}
