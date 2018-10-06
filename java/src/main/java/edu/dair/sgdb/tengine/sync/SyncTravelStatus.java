package edu.dair.sgdb.tengine.sync;

import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.GLogger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    public long sync_travel_master_start_time;
    public long sync_travel_master_stop_time;

    public Lock aLock;
    public Condition condVar;
    public boolean finished;

    //public Map sync_travel_status;
    public ArrayList<SingleStep> travelPlan;
    public ConcurrentHashMap<Integer, HashSet<ByteBuffer>> sync_travel_vertices;
    public ConcurrentHashMap<Integer, HashSet<SyncServerPair>> sync_ext_servers;

    public int current_step;

    public ArrayList<KeyValue> travel_results;

    public AtomicLong Edge2DstLocalCounter;

    public SyncTravelStatus(long tid) {
        this.travelId = tid;
        sync_travel_master_start_time = 0L;
        sync_travel_master_stop_time = 0L;

        this.aLock = new ReentrantLock();
        this.condVar = aLock.newCondition();
        this.finished = false;

        travelPlan = new ArrayList<>();
        //sync_travel_status = new HashMap<>();
        sync_travel_vertices = new ConcurrentHashMap<>();
        sync_ext_servers = new ConcurrentHashMap<>();
        travel_results = new ArrayList<>();
        Edge2DstLocalCounter = new AtomicLong(0);
    }

    public boolean is_step_started(int step) {
        if (step <= current_step)
            return true;
        return false;
    }

    public void set_current_step(int step) {
        this.current_step = step;
    }

    public void incrEdge2DstLocalCounter(int size) {
        this.Edge2DstLocalCounter.addAndGet(size);
    }

    public long getEdge2DstLocalCounter() {
        return this.Edge2DstLocalCounter.get();
    }

    public void add_vertex_to_current_step(int stepId, HashSet<ByteBuffer> sKeySet) {
        this.sync_travel_vertices.putIfAbsent(stepId, new HashSet<ByteBuffer>());
        this.sync_travel_vertices.get(stepId).addAll(sKeySet);
    }

    public HashSet<ByteBuffer> get_vertex_of_step(int stepId) {
        return this.sync_travel_vertices.get(stepId);
    }


    public void add_servers_to_sync_of_step(int stepId, int src, Set<Integer> sset, int type) {
        this.sync_ext_servers.putIfAbsent(stepId, new HashSet<SyncServerPair>());
        HashSet<SyncServerPair> sets = new HashSet<>();
        for (int dst : sset) {
            sets.add(new SyncServerPair(src, dst, type));
        }
        this.sync_ext_servers.get(stepId).addAll(sets);
    }

    public synchronized void remove_server_to_sync_of_step(int stepId, int src, int dst, int type) {
        SyncServerPair ext = new SyncServerPair(src, dst, type);
        this.sync_ext_servers.get(stepId).remove(ext);
    }

    public HashSet<SyncServerPair> get_servers_to_sync_of_step(int stepId) {
        return this.sync_ext_servers.get(stepId);
    }

    public boolean is_servers_to_sync_empty(int stepId) {
        return sync_ext_servers.get(stepId).isEmpty();
    }

    public void set_travel_plan(ArrayList<SingleStep> plan) {
        this.travelPlan = plan;
    }

    public ArrayList<SingleStep> get_travel_plan() {
        return this.travelPlan;
    }

    public synchronized void add_to_travel_results(List<KeyValue> vals) {
        travel_results.addAll(vals);
    }

}
