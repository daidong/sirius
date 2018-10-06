package edu.dair.sgdb.tengine.async;

import edu.dair.sgdb.tengine.travel.TravelDescriptor;
import edu.dair.sgdb.tengine.travel.Triple;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.GLogger;
import org.json.simple.JSONArray;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncTravelStatus {

    public class StepPayLoad {

        public HashSet<PVertex> vertices;
        public HashMap<Integer, Integer> epochIdPerServer;

        public StepPayLoad() {
            this.vertices = new HashSet<>();
            this.epochIdPerServer = new HashMap<>();
        }

        public void newVertex(PVertex pv) {
            this.vertices.add(pv);
        }

        public void addOrUpdateEpochId(int serverId, int epochId) {
            if (epochIdPerServer.containsKey(serverId)) {
                int currId = epochIdPerServer.get(serverId);
                if (currId < epochId) {
                    epochIdPerServer.put(serverId, epochId);
                }
            } else {
                epochIdPerServer.put(serverId, epochId);
            }
        }
    }

    public class PVertex {

        public ByteBuffer key;
        public byte p_ve;

        public PVertex(ByteBuffer k, boolean is_p_v) {
            this.key = k;
            if (is_p_v) {
                this.p_ve = (byte) 0;
            } else {
                this.p_ve = (byte) 1;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PVertex) {
                PVertex that = (PVertex) obj;
                if (this.key.equals(that.key) && this.p_ve == that.p_ve) {
                    return true;
                }
                return false;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return key.hashCode() + (int) p_ve;
        }
    }

    public class AsyncTravelPair {

        int startFrom, endTo;

        public AsyncTravelPair(int a, int b) {
            startFrom = a;
            endTo = b;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof AsyncTravelPair) {
                AsyncTravelPair that = (AsyncTravelPair) obj;
                return startFrom == that.startFrom && endTo == that.endTo;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return startFrom + endTo;
        }
    }

    public long asyncMasterStartAt = 0L;
    public long repeatedVisits = 0L;
    public long combinedVisits = 0L;
    public long totalVisits = 0L;

    public int totalSteps = 0;
    public ArrayList<KeyValue> travel_results;
    public CopyOnWriteArrayList<TravelDescriptor> travel_tracker;
    public ConcurrentSkipListMap<Integer, StepPayLoad> travelVertices;
    public Map travelContents;
    public HashSet<Triple> visitedBuffer;
    public AtomicLong Edge2DstLocalCounter;

    public ConcurrentHashMap<Integer, Boolean> step_finished;
    public ConcurrentHashMap<Integer, HashMap<AsyncTravelPair, Integer>> async_tracker;

    public AsyncTravelStatus() {
        this.asyncMasterStartAt = 0L;
        this.repeatedVisits = 0L;
        this.combinedVisits = 0L;
        this.totalVisits = 0L;

        this.travel_results = new ArrayList<>();
        this.travel_tracker = new CopyOnWriteArrayList<>();
        this.travelContents = null;
        this.travelVertices = new ConcurrentSkipListMap<>();
        this.visitedBuffer = new HashSet<>();

        Edge2DstLocalCounter = new AtomicLong(0);
        this.step_finished = new ConcurrentHashMap<>();
        this.async_tracker = new ConcurrentHashMap<>();
    }

    public int getTotalSteps() {
        return this.totalSteps;
    }

    public boolean isStepFinished(int stepId) {
        this.step_finished.putIfAbsent(stepId, false);
        return this.step_finished.get(stepId);
    }

    public void setStepFinished(int stepId) {
        this.step_finished.put(stepId, true);
    }

    // Coordinator uses these funtions to track traversal execution
    public synchronized void addToAsyncExtServers(int stepId, int src, Set<Integer> sset, int id) {
        this.async_tracker.putIfAbsent(stepId, new HashMap<AsyncTravelPair, Integer>());
        for (int dst : sset) {
            AsyncTravelPair atp = new AsyncTravelPair(src, dst);
            if (!this.async_tracker.get(stepId).containsKey(atp)) {
                this.async_tracker.get(stepId).put(atp, id);
            } else {
                int curr = this.async_tracker.get(stepId).get(atp);
                if (curr < id) {
                    this.async_tracker.get(stepId).put(atp, id);
                }
            }
        }
    }

    // By removing, we remove element if curr id is less or equal than new one;
    public synchronized void removeFromASyncExtServers(int stepId, int src, int dst, int id) {
        if (!this.async_tracker.containsKey(stepId)) {
            GLogger.error("No ASyncTravelStatus's async_tracker exist for stepId: %d", stepId);
            return;
        }
        AsyncTravelPair rm = new AsyncTravelPair(src, dst);
        if (!this.async_tracker.get(stepId).containsKey(rm)) {
            return;
        }
        int curr = this.async_tracker.get(stepId).get(rm);
        if (curr <= id) {
            this.async_tracker.get(stepId).remove(rm);
        } else {
            return;
        }
    }

    public boolean isASyncServerEmpty(int stepId) {
        if (!this.async_tracker.containsKey(stepId)) {
            GLogger.error("No ASyncTravelStatus's async server exist for stepId: %d", stepId);
            return false;
        }
        return async_tracker.get(stepId).isEmpty();
    }

    public void incrEdge2DstLocalCounter(int size) {
        this.Edge2DstLocalCounter.addAndGet(size);
    }

    public long getEdge2DstLocalCounter() {
        return this.Edge2DstLocalCounter.get();
    }

    public synchronized void addToVisitedBuffer(Triple t) {
        this.visitedBuffer.add(t);
    }

    public synchronized void addToTravelVertices(int stepId, HashSet<ByteBuffer> sKeySet,
            boolean p_v_or_e, int src, int id) {

        synchronized (this) {
            this.travelVertices.putIfAbsent(stepId, new StepPayLoad());
            StepPayLoad spl = this.travelVertices.get(stepId);
            for (ByteBuffer key : sKeySet) {
                spl.newVertex(new PVertex(key, p_v_or_e));
            }
            spl.addOrUpdateEpochId(src, id);
        }
    }

    public synchronized ConcurrentSkipListMap<Integer, StepPayLoad> getTravelVertices() {
        return this.travelVertices;
    }

    public synchronized void setTravelContents(Map contents) {
        this.travelContents = contents;
        JSONArray payload = (JSONArray) this.travelContents.get("travel_payload");
        totalSteps = (payload.size() - 1);
    }

    public synchronized void staticRecordOneVisit(int t) {
        //GLogger.debug("staticRecordOneVisit");
        this.totalVisits += t;
    }

    public synchronized void staticRecordOneRepeat(int t) {
        //GLogger.debug("staticRecordOneRepeat");
        this.repeatedVisits += t;
    }

    public synchronized boolean isVisited(Triple t) {
        return this.visitedBuffer.contains(t);
    }

    public synchronized void staticRecordStartTime(long ts) {
        this.asyncMasterStartAt = ts;
    }

    public synchronized long staticGetStartTime() {
        return this.asyncMasterStartAt;
    }

    public synchronized void addToTravelTracker(TravelDescriptor td) {
        this.travel_tracker.add(td);
    }

    public synchronized void removeTravelTracker(TravelDescriptor td) {
        this.travel_tracker.remove(td);
    }

    public synchronized void addTravelResults(List<KeyValue> set) {
        this.travel_results.addAll(set);
    }

    public synchronized long staticGetVisit() {
        return this.totalVisits;
    }

    public synchronized long staticGetRepeat() {
        return this.repeatedVisits;
    }

    public synchronized long staticGetCombine() {
        return this.combinedVisits;
    }

}
