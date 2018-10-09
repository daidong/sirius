package edu.dair.sgdb.tengine.bfs;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.tengine.TravelLocalReader;
import edu.dair.sgdb.tengine.travel.JSONCommand;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.TGraphFSServer;
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
    Lock alock = null;

    public bfs(AbstractSrv inst){
        this.instance = inst;
        this.vertices_to_travel = new HashMap<>();
        this.alock = new ReentrantLock();
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
                    a_cond.wait();
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
            alock.unlock();
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

        this.vertices_to_travel.putIfAbsent(tid, new HashSet<ByteBuffer>());
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        int master = instance.getLocalIdx();
        int current_step = 0;

        List<byte[]> keySet = travelPlan.get(current_step).vertexKeyRestrict.values();

        HashMap<Integer, HashSet<ByteBuffer>> servers_store_keys_next_step = get_servers_from_keys_1(keySet);
        for (int s : servers_store_keys_next_step.keySet()) {
            HashSet<ByteBuffer> keys_set = servers_store_keys_next_step.get(s);
            rpc_sync_travel_vertices(s, tid, current_step, keys_set, payload);
        }

        Set<Integer> servers_list_2 = servers_store_keys_next_step.keySet();


        while (current_step <= travelPlan.size()){
            Set<Integer> servers_list_1 = new HashSet<>(servers_list_2);
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

            current_step += 1;
        }

        int travel_time = (int) (System.currentTimeMillis() - bfs_start);
        return travel_time;
    }

    public int travel_vertices(long tid, int sid, Set<ByteBuffer> keys, String payload){
        alock.lock();
        this.vertices_to_travel.putIfAbsent(tid, new HashSet<ByteBuffer>());
        this.vertices_to_travel.get(tid).addAll(keys);
        alock.unlock();
        return 0;
    }

    public HashSet<ByteBuffer> travel_edges(long tid, int sid, Set<ByteBuffer> keys, String payload){
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        SingleStep currStep = travelPlan.get(sid);

        ArrayList<byte[]> passedVertices = new ArrayList<>();
        for (ByteBuffer k : keys)
            passedVertices.add(NIOHelper.getActiveArray(k));

        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
        HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(
                this.instance.localStore, passedVertices, currStep, 0);


        HashSet<ByteBuffer> next_vertices = new HashSet<>();
        for (byte[] v : nextVertices)
            next_vertices.add(ByteBuffer.wrap(v));

        return next_vertices;
    }

    public HashSet<Integer> travel_start_step(long tid, int sid, String payload){
        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        SingleStep currStep = travelPlan.get(sid);
        HashSet<ByteBuffer> keys = this.vertices_to_travel.get(tid);
        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(instance.localStore, keys, currStep, 0);
        HashMap<Integer, HashSet<ByteBuffer>> edges_and_servers = new HashMap<>();
        for (byte[] v : passedVertices){
            Set<Integer> srvs = instance.getEdgeLocs(v);
            for (int s : srvs){
                edges_and_servers.putIfAbsent(s, new HashSet<>());
                edges_and_servers.get(s).add(ByteBuffer.wrap(v));
            }
        }

        Set<Integer> edge_servers = edges_and_servers.keySet();

        int pending_to_finish = edge_servers.size();
        HashSet<ByteBuffer> vertices_for_next_step = new HashSet<>();
        lock_and_wait lw = new lock_and_wait(pending_to_finish);

        for (int s : edge_servers){
            instance.workerPool.execute(new thread_travel_edges(tid, sid, payload, edges_and_servers.get(s),
                    instance, lw, vertices_for_next_step, s));
        }

        lw.wait_until_finish();

        HashMap<Integer, HashSet<ByteBuffer>> servers_and_keys = get_servers_from_keys_2(vertices_for_next_step);
        for (int s : servers_and_keys.keySet()) {
            HashSet<ByteBuffer> keys_set = servers_and_keys.get(s);
            rpc_sync_travel_vertices(s, tid, sid, keys_set, payload);
        }

        return (HashSet<Integer>) edge_servers;
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

            lw.lock();
            next_servers.addAll(sets);
            lw.unlock();

            lw.finish_one();
        }
    }
    private String gen_json_string_from_travel_plan(ArrayList<SingleStep> travelPlan) {
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
                perServerVertices.putIfAbsent(s, new HashSet<>());
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
                perServerVertices.putIfAbsent(s, new HashSet<>());
                perServerVertices.get(s).add(ByteBuffer.wrap(key));
            }
        }
        return perServerVertices;
    }
}

