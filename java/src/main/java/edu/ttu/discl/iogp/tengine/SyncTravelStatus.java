package edu.ttu.discl.iogp.tengine;

import edu.ttu.discl.iogp.tengine.travel.SingleStep;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.GLogger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SyncTravelStatus {

    public class SyncServerPair {

        int startFrom, endTo, type;

        public SyncServerPair(int a, int b, int type) {
            startFrom = a;
            endTo = b;
            this.type = type;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof SyncServerPair) {
                SyncServerPair that = (SyncServerPair) obj;
                return startFrom == that.startFrom && endTo == that.endTo && type == that.type;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return startFrom + endTo + type;
        }

        @Override
        public String toString() {
            return (startFrom + "->" + endTo + "(" + type + ")");
        }

    }

    public long travelId;
    public long syncMasterStartAt;
    //public Map sync_travel_status;
    public ArrayList<SingleStep> travelPlan;
    public ConcurrentHashMap<Integer, HashSet<ByteBuffer>> sync_travel_vertices;
    public ConcurrentHashMap<Integer, HashSet<ByteBuffer>> sync_travel_edges;
    public ConcurrentHashMap<Integer, HashSet<SyncServerPair>> sync_ext_servers;
    public HashMap<Integer, Boolean> started;
    public ArrayList<KeyValue> travel_results;

    public AtomicLong Edge2DstLocalCounter;

    public SyncTravelStatus(long tid) {
        this.travelId = tid;
        syncMasterStartAt = 0L;
        travelPlan = new ArrayList<>();
        //sync_travel_status = new HashMap<>();
        sync_travel_vertices = new ConcurrentHashMap<>();
        sync_travel_edges = new ConcurrentHashMap<>();
        sync_ext_servers = new ConcurrentHashMap<>();
        travel_results = new ArrayList<>();
        Edge2DstLocalCounter = new AtomicLong(0);
        started = new HashMap<>();
    }

    public boolean isStepStarted(int step){
        synchronized (started) {
            if (this.started.containsKey(step))
                return this.started.get(step);
            else
                return false;
        }
    }
    public void setStepStarted(int step){
        synchronized (started) {
            this.started.put(step, true);
        }
    }

    public void incrEdge2DstLocalCounter(int size) {
        this.Edge2DstLocalCounter.addAndGet(size);
    }

    public long getEdge2DstLocalCounter() {
        return this.Edge2DstLocalCounter.get();
    }

    public void addToSyncTravelVertices(int stepId, HashSet<ByteBuffer> sKeySet) {
        this.sync_travel_vertices.putIfAbsent(stepId, new HashSet<ByteBuffer>());
        this.sync_travel_vertices.get(stepId).addAll(sKeySet);
    }

    public HashSet<ByteBuffer> getSyncTravelVertices(int stepId) {
        if (!this.sync_travel_vertices.containsKey(stepId)) {
            GLogger.error("No SyncTravelStatus's travel vertices exist for travelId: %d, stepId: %d", travelId, stepId);
            return null;
        }
        return this.sync_travel_vertices.get(stepId);
    }

    public void addToSyncTravelEdges(int stepId, HashSet<ByteBuffer> sKeySet) {
        this.sync_travel_edges.putIfAbsent(stepId, new HashSet<ByteBuffer>());
        this.sync_travel_edges.get(stepId).addAll(sKeySet);
    }

    public HashSet<ByteBuffer> getSyncTravelEdges(int stepId) {
        if (!this.sync_travel_edges.containsKey(stepId)) {
            GLogger.error("No SyncTravelStatus's travel vertices exist for travelId: %d, stepId: %d", travelId, stepId);
            return null;
        }
        return this.sync_travel_edges.get(stepId);
    }

    public void addToSyncServers(int stepId, int src, Set<Integer> sset, int type) {
        this.sync_ext_servers.putIfAbsent(stepId, new HashSet<SyncServerPair>());
        HashSet<SyncServerPair> sets = new HashSet<>();
        for (int dst : sset) {
            sets.add(new SyncServerPair(src, dst, type));
        }
        this.sync_ext_servers.get(stepId).addAll(sets);
    }

    public synchronized void removeFromSyncServers(int stepId, int src, int dst, int type) {
        if (!this.sync_ext_servers.containsKey(stepId)) {
            GLogger.error("No SyncTravelStatus's sync server exist for travelId: %d, stepId: %d", travelId, stepId);
            return;
        }
        SyncServerPair ext = new SyncServerPair(src, dst, type);
        this.sync_ext_servers.get(stepId).remove(ext);
    }

    public HashSet<SyncServerPair> getSyncServers(int stepId) {
        if (!this.sync_ext_servers.containsKey(stepId)) {
            GLogger.error("No SyncTravelStatus's sync server exist for travelId: %d, stepId: %d", travelId, stepId);
            return null;
        }
        return this.sync_ext_servers.get(stepId);
    }

    public boolean isSyncServerEmpty(int stepId) {
        if (!this.sync_ext_servers.containsKey(stepId)) {
            GLogger.error("No SyncTravelStatus's sync server exist for travelId: %d, stepId: %d", travelId, stepId);
            return false;
        }
        return sync_ext_servers.get(stepId).isEmpty();
    }

    /*
    public void setSyncTravelStatus(Map request) {
        this.sync_travel_status = request;
    }
    */
    public void setTravelPlan(ArrayList<SingleStep> plan){
        this.travelPlan = plan;
    }
    
    public ArrayList<SingleStep> getTravelPlan(){
        return this.travelPlan;
    }
    
    public synchronized void addToTravelResult(List<KeyValue> vals) {
        travel_results.addAll(vals);
    }

}
