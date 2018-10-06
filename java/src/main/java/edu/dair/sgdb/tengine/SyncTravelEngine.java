package edu.dair.sgdb.tengine;

import edu.dair.sgdb.tengine.prefetch.PreLoadMemoryPool;
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
import org.apache.thrift.async.AsyncMethodCallback;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
        return this.travel_status.get(travelId).isStepStarted(stepId);
    }

    public void setStepStarted(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return;
        }
        this.travel_status.get(travelId).setStepStarted(stepId);
    }

    public HashSet<ByteBuffer> getSyncTravelVertices(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).getSyncTravelVertices(stepId);
    }

    // Sync Server Operations
    private void addToSyncServers(long travelId, int stepId, int src, Set<Integer> sset, int type) {
        this.travel_status.putIfAbsent(travelId, new SyncTravelStatus(travelId));
        this.travel_status.get(travelId).addToSyncServers(stepId, src, sset, type);
    }

    private void removeFromSyncServers(long travelId, int stepId, int src, int dst, int type) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return;
        }
        this.travel_status.get(travelId).removeFromSyncServers(stepId, src, dst, type);
    }

    private boolean isSyncServerEmpty(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return false;
        }
        return this.travel_status.get(travelId).isSyncServerEmpty(stepId);
    }

    private HashSet<SyncTravelStatus.SyncServerPair> getSyncServers(long travelId, int stepId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).getSyncServers(stepId);
    }

    // Sync Results Operations
    private void addToTravelResults(long travelId, List<KeyValue> vals) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return;
        }
        this.travel_status.get(travelId).addToTravelResult(vals);
    }

    private void startSyncTravelTime(long travelId) {
        this.travel_status.putIfAbsent(travelId, new SyncTravelStatus(travelId));
        this.travel_status.get(travelId).syncMasterStartAt = System.currentTimeMillis();
    }

    // Sync Time Operations
    private long getStartSyncTravelTime(long travelId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return 0;
        }
        return this.travel_status.get(travelId).syncMasterStartAt;
    }

    private void setSyncTravelPlan(long travelId, ArrayList<SingleStep> plan) {
        this.travel_status.putIfAbsent(travelId, new SyncTravelStatus(travelId));
        this.travel_status.get(travelId).setTravelPlan(plan);
    }

    public ArrayList<SingleStep> getSyncTravelPlan(long travelId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus inst exists for travelId: %d", travelId);
            return null;
        }
        return this.travel_status.get(travelId).getTravelPlan();
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

    public String serializeTravelPlan(ArrayList<SingleStep> travelPlan) {
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

    public ArrayList<SingleStep> deSerializeTravelPlan(String payloadString) {
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
        int stepId, replyTo;
        long ts = tc.getTs();
        long travelId = (long) this.mid.incrementAndGet()
                         + ((long) (instance.getLocalIdx() + 1) * (1L << 32));
        startSyncTravelTime(travelId);

        GLogger.info("%d Recieve TravelMaster From %d At %d",
                instance.getLocalIdx(), -1, System.nanoTime());

        ArrayList<SingleStep> travelPlan = deSerializeTravelPlan(tc.getPayload());
        setSyncTravelPlan(travelId, travelPlan);

        stepId = 0;
        SingleStep firstStep = travelPlan.get(stepId);
        replyTo = instance.getLocalIdx();

        List<byte[]> keySet = firstStep.vertexKeyRestrict.values();
        GLogger.debug("[%d Coordinator] Receives Travel %d on %s at %d",
                instance.getLocalIdx(), travelId,
                new String(keySet.get(0)),
                getStartSyncTravelTime(travelId));

        for (byte[] t : keySet) {
            this.tsrcs.put(travelId, t);
        }

        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = getVertexBroadcastServers(keySet);

        //register the execution
        //we are extending vertices
        addToSyncServers(travelId, stepId, replyTo, perServerVertices.keySet(), 0);
        GLogger.info("In Step %d, Server[%d] extend to Servers %s",
                stepId,
                instance.getLocalIdx(),
                perServerVertices.keySet());

        // broadcast vertices to servers;
        // these servers will cache current step and wait for the SYNC_TRAVEL_START command;
        for (int s : perServerVertices.keySet()) {
            List<byte[]> nextKeys = new ArrayList<byte[]>();
            for (ByteBuffer bb : perServerVertices.get(s)) {
                byte[] tbb = NIOHelper.getActiveArray(bb);
                nextKeys.add(tbb);
            }

            travelPlan.get(stepId).vertexKeyRestrict =
                    new SingleRestriction.InWithValues("key".getBytes(), nextKeys);
            String travelPayLoad = serializeTravelPlan(travelPlan);

            TravelCommand tc1 = new TravelCommand();
            tc1.setType(TravelCommandType.SYNC_TRAVEL)
                    .setTravelId(travelId)
                    .setStepId(stepId)
                    .setReply_to(replyTo)
                    .setGet_from(instance.getLocalIdx())
                    .setLocal_id(instance.getLocalIdx())
                    .setTs(ts)
                    .setPayload(travelPayLoad)
                    .setSub_type(0);

            GLogger.info("%d Send Travel Command to %d to visit vertex at %d",
                    instance.getLocalIdx(), s, System.nanoTime());

            if (s != instance.getLocalIdx()) {
                TGraphFSServer.Client client = instance.getClientConn(s);
                client.syncTravel(tc1);
                instance.releaseClientConn(s, client);
            } else {
                syncTravel(tc1);
            }
        }


        setStepStarted(travelId, stepId);
        // broadcast SYNC_TRAVEL_START to start next round of synchronous traversal.
        TravelCommand tc1 = new TravelCommand();
        tc1.setType(TravelCommandType.SYNC_TRAVEL_START)
                .setTravelId(travelId)
                .setStepId(stepId)
                .setReply_to(replyTo)
                .setGet_from(instance.getLocalIdx())
                .setTs(ts);

        for (int s : perServerVertices.keySet()) {
            GLogger.info("%d Send TravelStart to %d at %d",
                    instance.getLocalIdx(), s, System.nanoTime());

            if (s != instance.getLocalIdx()) {
                TGraphFSServer.Client client = instance.getClientConn(s);
                client.syncTravelStart(tc1);
                instance.releaseClientConn(s, client);
            } else {
                syncTravelStart(tc1);
            }
        }

        return 0;
    }

    public synchronized int syncTravel(TravelCommand tc) throws TException {
        UUID id = UUID.randomUUID();
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int subType = tc.getSub_type();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();
        GLogger.debug("[%d] [travel] [%s] [Step] [%d] Enter",
                instance.getLocalIdx(), id.toString(), stepId);

        GLogger.info("%d Receive Travel Command Type: %d from %d At %d",
                instance.getLocalIdx(), subType, getFrom, System.nanoTime());

        String payloadString = tc.getPayload();
        ArrayList<SingleStep> travelPlan = deSerializeTravelPlan(payloadString);
        setSyncTravelPlan(travelId, travelPlan);

        SingleStep currStep = travelPlan.get(stepId);
        HashSet<ByteBuffer> kSets = new HashSet<ByteBuffer>();
        for (byte[] key : currStep.vertexKeyRestrict.values()) {
            kSets.add(ByteBuffer.wrap(key));
        }

        // There are two types of SYNC_TRAVEL commands.
        // One for vertex processing; one for edge processing;
        if (subType == 0) {
            this.travel_status.get(travelId).addToSyncTravelVertices(stepId, kSets);

        } else {
            instance.workerPool.execute(
                    new SyncTravelEdgeWorker(
                            instance,
                            this,
                            travelId,
                            stepId,
                            getFrom,
                            replyTo,
                            ts,
                            kSets
                    )
            );
        }

        GLogger.debug("[%d] [travel] [%s] [Step] [%d] Finish",
                instance.getLocalIdx(), id.toString(), stepId);
        return 0;
    }

    public synchronized int syncTravelStart(TravelCommand tc) throws TException {
        setStepStarted(tc.getTravelId(), tc.getStepId());

        instance.workerPool.execute(
                new SyncTravelVertexWorker(
                        instance,
                        this,
                        tc.getTravelId(),
                        tc.getStepId(),
                        tc.getReply_to(),
                        tc.getGet_from(),
                        tc.getTs()
                )
        );

        GLogger.info("%d Receive TravelStart from %d at %d",
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
            removeFromSyncServers(travelId, stepId, i, dstServer, 0);
        }
        //GLogger.warn("[%d] stepId[%d] sync servers: %s",
        //        inst.getLocalIdx(), stepId, getSyncServers(travelId, stepId));

        //removeFromSyncServers(travelId, stepId, srcServer, dstServer, 0);
        if (tc.isSetVals()) {
            List<KeyValue> vals = tc.getVals();
            addToTravelResults(travelId, vals);
        }

        long costTime = System.currentTimeMillis() - getStartSyncTravelTime(travelId);


        if (isSyncServerEmpty(travelId, stepId)) {
            GLogger.info("in SyncTravelRtn, Step %d Finishes at %d", stepId, costTime);
            GLogger.info("[%d] TravelId[%d] Starting %s, costs: %d",
                    instance.getLocalIdx(), travelId,
                    new String(this.tsrcs.get(travelId)),
                    costTime);
            instance.workerPool.execute(new DeleteTravelInstance(instance, travelId));
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
        addToSyncServers(travelId, stepId, getFrom, srvs, subType);

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
                removeFromSyncServers(travelId, stepId, i, dst, type);
            }

            //GLogger.warn("[%d] stepId[%d] sync servers: %s",
            //        inst.getLocalIdx(), stepId, getSyncServers(travelId, stepId));

        } else {
            //GLogger.warn("[%d] stepId[%d] receives %d -> %d (%d)",
            //        inst.getLocalIdx(), stepId, src, dst, type);

            removeFromSyncServers(travelId, stepId, src, dst, type);

            //GLogger.warn("[%d] stepId[%d] sync servers: %s",
            //        inst.getLocalIdx(), stepId, getSyncServers(travelId, stepId));

            long costTime = System.currentTimeMillis() - getStartSyncTravelTime(travelId);

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
            }
        }

        return 0;
    }


}
