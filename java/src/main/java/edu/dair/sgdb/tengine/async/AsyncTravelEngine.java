package edu.dair.sgdb.tengine.async;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.tengine.travel.*;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.GLogger;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.dair.sgdb.utils.NIOHelper.getActiveArray;

public class AsyncTravelEngine {

    private AbstractSrv instance;
    public ConcurrentHashMap<Long, AsyncTravelStatus> travel_status;
    public LinkedBlockingDeque<Long> travels;
    public AtomicInteger Incr_Id;
    public HashMap<Long, Object> locks;
    public HashMap<Long, byte[]> tsrcs;
    public AtomicInteger mid;

    public AsyncTravelEngine(AbstractSrv s) {
        this.instance = s;
        this.travels = new LinkedBlockingDeque<>();
        this.travel_status = new ConcurrentHashMap<>();
        s.workerPool.execute(new AsyncTravelWorker(s, this));
        this.Incr_Id = new AtomicInteger(0);
        this.locks = new HashMap<Long, Object>();
        this.tsrcs = new HashMap<>();
        this.mid = new AtomicInteger(1);
    }

    private Map buildTravelContent(String payloadString, int replyTo, long ts) {
        JSONCommand js = new JSONCommand();
        Map request = js.parse(payloadString);
        request.put("reply_to", replyTo);
        request.put("ts", ts);
        return request;
    }

    private ArrayList<SingleStep> deSerializeTravelPlan(String payloadString) {
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

    public int getEpoch() {
        return this.Incr_Id.addAndGet(1);
    }

    /**
     * Async Traversal Status Processing
     */
    public int getTotalSteps(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return -1;
        }
        return this.travel_status.get(tid).getTotalSteps();
    }

    public boolean isStepFinished(long tid, int stepId) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return false;
        }
        return this.travel_status.get(tid).isStepFinished(stepId);
    }

    public void setStepFinished(long tid, int stepId) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return;
        }
        this.travel_status.get(tid).setStepFinished(stepId);
    }

    public void addToASyncServers(long tid, int stepId, int src, HashSet<Integer> srvs, int id) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).addToAsyncExtServers(stepId, src, srvs, id);
    }

    public void removeFromASyncExtServers(long tid, int stepId, int src, int dst, int id) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return;
        }
        this.travel_status.get(tid).removeFromASyncExtServers(stepId, src, dst, id);
    }

    public boolean isASyncServerEmpty(long tid, int stepId) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return false;
        }
        return this.travel_status.get(tid).isASyncServerEmpty(stepId);
    }

    //Visited Buffer Processing
    public void addToVisitedBuffer(Long tid, Triple t) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).addToVisitedBuffer(t);
    }

    //New travable vertices
    public synchronized void addVerticesWithEpoch(long tid, int stepId, HashSet<ByteBuffer> sKeySet, boolean p_or_e,
            int src, int epochId) {
        AsyncTravelStatus ats = this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        synchronized (ats) {
            ats.addToTravelVertices(stepId, sKeySet, p_or_e, src, epochId);
        }
        if (!travels.contains(tid)) {
            travels.offerFirst(tid);
        }
    }

    public ConcurrentSkipListMap<Integer, AsyncTravelStatus.StepPayLoad> getStepPayLoad(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return null;
        }
        return this.travel_status.get(tid).getTravelVertices();
    }

    public void setTravelContents(long tid, Map contents) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).setTravelContents(contents);
    }

    public Map getTravelContents(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return null;
        }
        return this.travel_status.get(tid).travelContents;
    }

    public void staticRecordOneVisit(long tid) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).staticRecordOneVisit(1);
    }

    public void staticRecordOneRepeat(long tid) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).staticRecordOneRepeat(1);
    }

    public void staticRecordStartTime(long tid, long ts) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).staticRecordStartTime(ts);
    }

    public long staticGetStartTime(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return 0;
        }
        return this.travel_status.get(tid).staticGetStartTime();
    }

    public long staticGetVisit(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return 0;
        }
        return this.travel_status.get(tid).staticGetVisit();
    }

    public long staticGetRepeat(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return 0;
        }
        return this.travel_status.get(tid).staticGetRepeat();
    }

    public long staticGetCombine(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return 0;
        }
        return this.travel_status.get(tid).staticGetCombine();
    }

    public boolean isVisited(long tid, Triple t) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return false;
        }
        return this.travel_status.get(tid).isVisited(t);
    }

    public void addToTravelTracker(long tid, TravelDescriptor td) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
        }
        this.travel_status.get(tid).addToTravelTracker(td);
    }

    public void removeTravelTracker(long tid, TravelDescriptor td) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
        }
        this.travel_status.get(tid).removeTravelTracker(td);
    }

    public void addTravelResults(long tid, List<KeyValue> set) {
        this.travel_status.putIfAbsent(tid, new AsyncTravelStatus());
        this.travel_status.get(tid).addTravelResults(set);
    }

    public List<KeyValue> getTravelResults(long tid) {
        if (!this.travel_status.containsKey(tid)) {
            GLogger.error("Travel Status Not Exist Error");
            return null;
        }
        return this.travel_status.get(tid).travel_results;
    }

    // ASync Local Counter
    public void incrEdge2DstLocalCounter(long travelId, int s) {
        this.travel_status.putIfAbsent(travelId, new AsyncTravelStatus());
        this.travel_status.get(travelId).incrEdge2DstLocalCounter(s);
    }

    public long getEdge2DstLocalCounter(long travelId) {
        if (!this.travel_status.containsKey(travelId)) {
            GLogger.error("No SyncTravelStatus instance exists for travelId: %d", travelId);
            return 0L;
        }
        return this.travel_status.get(travelId).getEdge2DstLocalCounter();
    }

    /**
     * Asynchronous Travel Processing Functions
     */
    public int deleteTravelInstance(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        this.travel_status.remove(travelId);
        return 0;
    }

    public synchronized int travel(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int replyTo = tc.getReply_to();
        int getFrom = tc.getGet_from();
        long ts = tc.getTs();
        List<ByteBuffer> sKeySet = tc.getKeys();
        Map r = buildTravelContent(tc.getPayload(), replyTo, ts);
        int subType = tc.getSub_type();
        int epochId = tc.getLocal_id(); //we use local_id to store epoch Id.

        setTravelContents(travelId, r);

        GLogger.debug("[%d] travelId[%d] at stepId[%d] From[%d] Keys[%d] Type[%d] EpochId[%d]",
                instance.getLocalIdx(), travelId, stepId, getFrom, sKeySet.size(), subType, epochId);
        GLogger.info("R TV %d %d %d %d", instance.getLocalIdx(), getFrom, System.nanoTime(), subType);        

        HashSet<ByteBuffer> keys = new HashSet<ByteBuffer>();
        for (ByteBuffer bkey : sKeySet) {
            byte[] key = getActiveArray(bkey);
            staticRecordOneVisit(travelId);
            Triple t = new Triple(travelId, stepId, key, (byte) subType);

            GLogger.debug("[%d] bkey: %s, key: %s isVisited?[%s]",
                    instance.getLocalIdx(), bkey, new String(key), isVisited(travelId, t));

            if (isVisited(travelId, t)) {
                staticRecordOneRepeat(travelId);
                continue;
            }
            keys.add(bkey);
            addToVisitedBuffer(travelId, t);
        }

        //we have to add new vertices and also updated the epoch inside locks
        addVerticesWithEpoch(travelId, stepId, keys, (subType == 0), getFrom, epochId);
        return 0;
    }

    public int travelMaster(TravelCommand tc) throws TException {
        long time = System.currentTimeMillis();
        int stepId, replyTo;
        long ts = tc.getTs();
        long travelId = (long) this.mid.incrementAndGet()
                + ((long) (instance.getLocalIdx() + 1) * (1L << 32));

        staticRecordStartTime(travelId, time);
        this.locks.put(travelId, new Object());
        GLogger.info("R TM %d %d %d", instance.getLocalIdx(), -1, System.nanoTime());

        GLogger.debug("[%d] Coordinator receives request at %d", instance.getLocalIdx(), staticGetStartTime(travelId));

        ArrayList<SingleStep> travelPlan = deSerializeTravelPlan(tc.getPayload());
        stepId = 0;
        SingleStep firstStep = travelPlan.get(stepId);
        replyTo = instance.getLocalIdx();

        Restriction vKeys = firstStep.vertexKeyRestrict;
        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
        for (byte[] key : vKeys.values()) {
            this.tsrcs.put(travelId, key);
            Set<Integer> servers = instance.getVertexLoc(key);
            for (int s : servers) {
                if (!perServerVertices.containsKey(s)) {
                    perServerVertices.put(s, new HashSet<ByteBuffer>());
                }
                perServerVertices.get(s).add(ByteBuffer.wrap(key));
            }
        }

        //Tell coordinator that we started new epoch
        int curr_epoch = this.getEpoch();
        List<Integer> extSrvs = new ArrayList<>(perServerVertices.keySet());

        //GLogger.debug("[%d] TravelMaster Regs Epoch: %d --> %s. EpochId = %d", instance.getLocalIdx(),
        //       this.instance.getLocalIdx(), extSrvs, curr_epoch);
        /*
         TravelCommand tc1 = new TravelCommand();
         tc1.setType(TravelCommandType.TRAVEL_REG).setTravelId(travelId).setStepId(stepId)
         .setReply_to(replyTo).setGet_from(this.instance.getLocalIdx()).setExt_srv(extSrvs)
         .setSub_type(1).setLocal_id(curr_epoch);
         travelReg(tc1);
         */
        addToASyncServers(travelId, stepId, this.instance.getLocalIdx(), new HashSet<>(extSrvs), curr_epoch);

        for (int s : extSrvs) {
            List<ByteBuffer> nextKeys = new ArrayList<>(perServerVertices.get(s));
            TravelCommand tc2 = new TravelCommand();
            tc2.setType(TravelCommandType.TRAVEL).setTravelId(travelId).setStepId(0)
                    .setReply_to(replyTo).setGet_from(instance.getLocalIdx())
                    .setTs(ts)
                    .setPayload(tc.getPayload()).setKeys(nextKeys)
                    .setSub_type(0)
                    .setLocal_id(curr_epoch);

            GLogger.info("S TV %d %d %d %d", instance.getLocalIdx(), s, System.nanoTime(), 0);
            if (s != instance.getLocalIdx()) {
                TGraphFSServer.Client client = instance.getClientConn(s);
                synchronized (client) {
                    client.travel(tc2);
                }
                instance.releaseClientConn(s, client);
            } else {
                travel(tc2);
            }
        }

        setStepFinished(travelId, -1);

        Object lock = locks.get(travelId);
        synchronized (lock) {
            try {
                lock.wait();
            } catch (InterruptedException ex) {
                Logger.getLogger(AsyncTravelEngine.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return 0;
    }

    public synchronized int travelRtn(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int dst = tc.getGet_from();
        List<EpochEntity> processedEpochs = tc.getEpoch();

        GLogger.info("R TR %d %d %d", instance.getLocalIdx(), dst, System.nanoTime());
        if (tc.isSetVals()) {
            addTravelResults(travelId, tc.getVals());
        }

        for (EpochEntity ee : processedEpochs) {
            int src = ee.serverId;
            int epoch = ee.epoch;
            removeFromASyncExtServers(travelId, stepId, src, dst, epoch);
        }

        long costTime = System.currentTimeMillis() - staticGetStartTime(travelId);

        if (isStepFinished(travelId, (stepId - 1)) && isASyncServerEmpty(travelId, stepId)) {
            GLogger.debug("Step-[%d] Finish at %d", stepId, costTime);
            setStepFinished(travelId, stepId);
        }

        if (isStepFinished(travelId, getTotalSteps(travelId))) {
            GLogger.info("[%d] travelId[%d] Starting %s, Return %d elements at %d. "
                    + "[STATISTICS] Repeated[%d], Combined[%d], Total[%d], Edge->Dst Local Counter[%d]",
                    instance.getLocalIdx(), travelId,
                    new String(this.tsrcs.get(travelId)),
                    getTravelResults(travelId).size(), costTime,
                    staticGetRepeat(travelId),
                    staticGetCombine(travelId),
                    staticGetVisit(travelId),
                    getEdge2DstLocalCounter(travelId));
        }

        Object lock = this.locks.get(travelId);
        synchronized (lock) {
            lock.notify();
        }

        return 0;
    }

    public synchronized int travelReg(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int src = tc.getGet_from();
        int epoch = tc.getLocal_id();
        HashSet<Integer> srvs = new HashSet<>(tc.getExt_srv());

        GLogger.debug("[%d] travelId[%d] stepId[%d] %d --> %s (%d) Reg",
                instance.getLocalIdx(), travelId, stepId, src, srvs, epoch);
        GLogger.info("R TE %d %d %d", instance.getLocalIdx(), src, System.nanoTime());
        
        addToASyncServers(travelId, stepId, src, srvs, epoch);

        return 0;
    }

    public synchronized int travelFin(TravelCommand tc) throws TException {
        long travelId = tc.getTravelId();
        int stepId = tc.getStepId();
        int dst = tc.getGet_from();
        List<EpochEntity> processedEpochs = tc.getEpoch();

        GLogger.debug("[%d] travelId[%d] stepId[%d] %s Fin",
                instance.getLocalIdx(), travelId, stepId, dst);
        GLogger.info("R TF %d %d %d", instance.getLocalIdx(), dst, System.nanoTime());
        
        for (EpochEntity ee : processedEpochs) {
            int src = ee.serverId;
            int epoch = ee.epoch;
            removeFromASyncExtServers(travelId, stepId, src, dst, epoch);
        }

        long costTime = System.currentTimeMillis() - staticGetStartTime(travelId);
        if (isStepFinished(travelId, (stepId - 1)) && isASyncServerEmpty(travelId, stepId)) {
            GLogger.debug("Step-[%d] Finish at %d", stepId, costTime);
            setStepFinished(travelId, stepId);
        }
        return 0;
    }

}
