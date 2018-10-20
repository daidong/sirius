package edu.dair.sgdb.tengine.bfs;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.tengine.TravelLocalReader;
import edu.dair.sgdb.tengine.travel.JSONCommand;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class bfs {

    private AbstractSrv instance;
    HashMap<Long, HashSet<ByteBuffer>> vertices_to_travel = null;
    Lock lock_vertices_to_travel = null;

    boolean preload_enabled = true;
    HashSet<ByteBuffer> preloaded_caches = null;
    Lock lock_preloaded_caches = null;
    ArrayList<Thread> preload_threads = null;

    boolean manual_delay = false;

    public bfs(AbstractSrv inst){
        this.instance = inst;
        this.vertices_to_travel = new HashMap<>();
        this.lock_vertices_to_travel = new ReentrantLock();
        this.preloaded_caches = new HashSet<ByteBuffer>();
        this.lock_preloaded_caches = new ReentrantLock();
        this.preload_threads = new ArrayList<>();
    }

    public class thread_prefetcher extends Thread {
        int sid;
        Set<ByteBuffer> keys;
        String payload;

        public thread_prefetcher(int sid, Set<ByteBuffer> keys, String payload){
            this.sid = sid;
            this.keys = keys;
            this.payload = payload;
        }

        //while next step has not started, load data into the cache.
        @Override
        public void run() {
            ArrayList<byte[]> passedVertices = null;
            try {
                ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
                SingleStep currStep = travelPlan.get(sid);
                passedVertices = TravelLocalReader.filterVertices_interruptable(instance.localStore, keys, currStep, 0, this);
            } catch (InterruptedException e) {
                GLogger.debug("Prefetch Thread is Stopped");
            } finally {
                lock_preloaded_caches.lock();
                if (passedVertices != null) {
                    for (byte[] v : passedVertices) {
                        preloaded_caches.add(ByteBuffer.wrap(v));
                    }
                }
                lock_preloaded_caches.unlock();
            }
        }
    }

    private class lock_and_wait{
        Lock a_lock;
        Condition a_cond;
        int pendings;

        public lock_and_wait(int pendings){
            a_lock = new ReentrantLock();
            a_cond = a_lock.newCondition();
            this.pendings = pendings;
        }

        public void wait_until_finish(){
            a_lock.lock();
            while (pendings != 0) {
                try {
                    a_cond.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    a_lock.unlock();
                }
            }
        }

        public void finish_one(){
            a_lock.lock();
            pendings -= 1;
            if (pendings == 0){
                a_cond.signal();
            }
            a_lock.unlock();
        }

        public void lock(){
            a_lock.lock();
        }
        public void unlock(){
            a_lock.unlock();
        }
    }

    public int travel_master(long tid, String payload){
        long bfs_start = System.currentTimeMillis();

        this.vertices_to_travel.put(tid, new HashSet<ByteBuffer>());
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        int current_step = 0;

        List<byte[]> keySet = travelPlan.get(current_step).vertexKeyRestrict.values();

        HashMap<Integer, HashSet<ByteBuffer>> servers_store_keys_next_step = get_servers_from_keys_1(keySet);
        for (int s : servers_store_keys_next_step.keySet()) {
            HashSet<ByteBuffer> keys_set = servers_store_keys_next_step.get(s);
            rpc_sync_travel_vertices(s, tid, current_step, keys_set, payload);
        }

        Set<Integer> servers_list_2 = new HashSet<>(servers_store_keys_next_step.keySet());

        while (current_step < travelPlan.size()){
            GLogger.info("Start Step[%d]", current_step);

            HashSet<Integer> servers_list_1 = new HashSet<>(servers_list_2);
            servers_list_2.clear();

            int pending_to_finish = servers_list_1.size();
            lock_and_wait lw = new lock_and_wait(pending_to_finish);

            for (int s : servers_list_1){
                instance.workerPool.execute(new thread_start_step(tid,
                        current_step, payload, instance,
                        lw,
                        servers_list_2, s));
            }

            lw.wait_until_finish();
            GLogger.info("Finish Step[%d]", current_step);
            current_step += 1;
        }

        int travel_time = (int) (System.currentTimeMillis() - bfs_start);
        GLogger.info("Travel Time: %d", travel_time);
        return travel_time;
    }

    public int travel_vertices(long tid, int sid, Set<ByteBuffer> keys, String payload){
        lock_vertices_to_travel.lock();
        if (!this.vertices_to_travel.containsKey(tid))
            this.vertices_to_travel.put(tid, new HashSet<ByteBuffer>());
        this.vertices_to_travel.get(tid).addAll(keys);
        lock_vertices_to_travel.unlock();

        if (preload_enabled) {
            /* start a thread to do some prefetching before travel_start_step arrives
             * @reminder: we copy keys to the thread to avoid conflict because other threads may change
             *            this.vertices_to_travel during prefetching.
             */
            Thread t = new thread_prefetcher(sid, new HashSet<ByteBuffer>(keys), payload);
            this.preload_threads.add(t);
            t.start();
        }

        return 0;
    }

    public HashSet<ByteBuffer> travel_edges(long tid, int sid, Set<ByteBuffer> keys, String payload){
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        SingleStep currStep = travelPlan.get(sid);

        ArrayList<byte[]> passedVertices = new ArrayList<>();
        for (ByteBuffer k : keys)
            passedVertices.add(NIOHelper.getActiveArray(k));

        HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(
                this.instance.localStore, passedVertices, currStep, 0);

        HashSet<ByteBuffer> next_vertices = new HashSet<>();
        for (byte[] v : nextVertices)
            next_vertices.add(ByteBuffer.wrap(v));

        return next_vertices;
    }

    public HashSet<Integer> travel_start_step(long tid, int sid, String payload){
        
        // shutdown the prefetcher
        for (Thread t : this.preload_threads) {
            t.interrupt();
        }
        this.preload_threads.clear();

        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        SingleStep currStep = travelPlan.get(sid);
        HashSet<ByteBuffer> keys = this.vertices_to_travel.get(tid);
        int before_checking_cache = keys.size();

        if (preload_enabled) {
            // when we can grab the lock, this indicates the prefetch thread has successfully exited.
            this.lock_preloaded_caches.lock();
            /* read data from preloaded data cache first */
            keys.removeAll(this.preloaded_caches);
            this.lock_preloaded_caches.unlock();
            GLogger.info("Step[%d] read preloaded data %d", sid, (before_checking_cache - keys.size()));
        }

        if (manual_delay) {
            // @test: manually add some latency on servers based on current step.
            if (instance.getLocalIdx() == (sid % instance.serverNum)) {
                try {
                    Thread.sleep(10);
                    GLogger.info("Read Vertices Delayed %d ms", 10);
                } catch (InterruptedException ie) {
                }
            }
        }

        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(instance.localStore, keys, currStep, 0);

        if (preload_enabled) {
            // add back preloaded vertices, so that the passedVertices has all vertices for current step.
            for (ByteBuffer bb : this.preloaded_caches) {
                passedVertices.add(NIOHelper.getActiveArray(bb));
            }
        }

        HashMap<Integer, HashSet<ByteBuffer>> edges_and_servers = new HashMap<>();
        for (byte[] v : passedVertices){
            Set<Integer> srvs = instance.getEdgeLocs(v);
            for (int s : srvs){
                if (!edges_and_servers.containsKey(s))
                    edges_and_servers.put(s, new HashSet<ByteBuffer>());
                edges_and_servers.get(s).add(ByteBuffer.wrap(v));
            }
        }

        Set<Integer> edge_servers = new HashSet<>(edges_and_servers.keySet());

        int pending_to_finish = edge_servers.size();
        HashSet<ByteBuffer> vertices_for_next_step = new HashSet<>();
        lock_and_wait lw = new lock_and_wait(pending_to_finish);

        for (int s : edge_servers){
            instance.workerPool.execute(new thread_travel_edges(tid, sid, payload, edges_and_servers.get(s),
                    instance, lw, vertices_for_next_step, s));
        }

        lw.wait_until_finish();

        HashSet<Integer> next_vertices = new HashSet<>();

        HashMap<Integer, HashSet<ByteBuffer>> servers_and_keys = get_servers_from_keys_2(vertices_for_next_step);
        for (int s : servers_and_keys.keySet()) {
            next_vertices.add(s);
            HashSet<ByteBuffer> keys_set = servers_and_keys.get(s);
            rpc_sync_travel_vertices(s, tid, sid, keys_set, payload);
        }

        if (preload_enabled) {
            //clear the preloaded_caches
            this.lock_preloaded_caches.lock();
            this.preloaded_caches.clear();
            this.lock_preloaded_caches.unlock();
        }

        return next_vertices;
    }


    private Set<ByteBuffer> rpc_sync_travel_edges(int server_id, long tid, int sid,
                                                      HashSet<ByteBuffer> keysets, String payload){
        TGraphFSServer.Client client = null;
        Set<ByteBuffer> vs = null;
        try {
            client = instance.getClientConn(server_id);
            vs = client.travel_edges(tid, sid, keysets, payload);
            instance.releaseClientConn(server_id, client);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return vs;
    }
    private void rpc_sync_travel_vertices(int server_id, long tid, int sid, HashSet<ByteBuffer> keys, String payload){
        TGraphFSServer.Client client = null;
        try {
            client = instance.getClientConn(server_id);
            client.travel_vertices(tid, sid, keys, payload);
            instance.releaseClientConn(server_id, client);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private Set<Integer> rpc_sync_travel_start_step(int server_id, long tid, int sid, String payload){
        TGraphFSServer.Client client = null;
        Set<Integer> srvs = null;
        try {
            client = instance.getClientConn(server_id);
            srvs = client.travel_start_step(tid, sid, payload);
            instance.releaseClientConn(server_id, client);

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        return srvs;
    }

    private class thread_travel_edges implements Runnable{
        long tid;
        int sid, server_id;
        String payload;
        AbstractSrv instance;
        HashSet<ByteBuffer> next_vertices;
        HashSet<ByteBuffer> keys;
        lock_and_wait lw;

        public thread_travel_edges(long tid, int sid, String payload, HashSet<ByteBuffer> keys,
                                   AbstractSrv inst,
                                   lock_and_wait lw,
                                   HashSet<ByteBuffer> next_vs,
                                   int server_id){
            this.tid = tid;
            this.sid = sid;
            this.payload = payload;
            this.keys = keys;
            this.instance = inst;
            this.lw = lw;
            this.next_vertices = next_vs;
            this.server_id = server_id;
        }

        @Override
        public void run() {
            Set<ByteBuffer> vs = rpc_sync_travel_edges(server_id, tid, sid, keys, payload);
            lw.lock();
            this.next_vertices.addAll(vs);
            lw.unlock();

            lw.finish_one();
        }
    }
    private class thread_start_step implements Runnable {

        long tid;
        int sid, server_id;
        String payload;
        AbstractSrv instance;
        Set<Integer> next_servers;
        lock_and_wait lw;

        public thread_start_step(long tid, int sid, String payload, AbstractSrv inst,
                                 lock_and_wait lw,
                                 Set<Integer> next_servers,
                                 int server_id){
            this.tid = tid;
            this.sid = sid;
            this.payload = payload;
            this.instance = inst;
            this.lw = lw;
            this.next_servers = next_servers;
            this.server_id = server_id;
        }

        @Override
        public void run() {
            Set<Integer> sets = rpc_sync_travel_start_step(server_id, tid, sid, payload);

            if (sets != null) {
                lw.lock();
                next_servers.addAll(sets);
                lw.unlock();
            }

            lw.finish_one();
        }
    }

    private ArrayList<SingleStep> build_travel_plan_from_json_string(String payloadString) {
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

    private HashMap<Integer, HashSet<ByteBuffer>> get_servers_from_keys_1(List<byte[]> keySet) {
        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
        for (byte[] key : keySet) {
            Set<Integer> servers = instance.getVertexLoc(key);
            for (int s : servers) {
                if (!perServerVertices.containsKey(s))
                    perServerVertices.put(s, new HashSet<ByteBuffer>());
                perServerVertices.get(s).add(ByteBuffer.wrap(key));
            }
        }
        return perServerVertices;
    }

    private HashMap<Integer, HashSet<ByteBuffer>> get_servers_from_keys_2(Set<ByteBuffer> keySet) {
        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
        for (ByteBuffer bkey : keySet) {
            byte[] key = NIOHelper.getActiveArray(bkey);
            Set<Integer> servers = instance.getVertexLoc(key);
            for (int s : servers) {
                if (!perServerVertices.containsKey(s))
                    perServerVertices.put(s, new HashSet<ByteBuffer>());
                perServerVertices.get(s).add(ByteBuffer.wrap(key));
            }
        }
        return perServerVertices;
    }
}

