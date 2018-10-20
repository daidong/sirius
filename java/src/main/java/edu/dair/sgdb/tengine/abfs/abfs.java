package edu.dair.sgdb.tengine.abfs;

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

public class abfs {

    private AbstractSrv instance;
    HashMap<Long, Set<Long>> running_tasks = null;
    HashMap<Long, Integer> total_steps = null;
    HashMap<Long, String> travel_payloads = null;
    HashMap<Long, lock_and_wait> locks = null;
    Lock lock_to_travels = null;

    HashMap<Integer, HashSet<ByteBuffer>> pass_vertices_cache = null;
    HashMap<Integer, HashSet<ByteBuffer>> sent_vertices_cache = null;

    TravelBook vertex_book;
    TravelBook edge_book;

    boolean manual_delay = false;

    private class TravelBook{
        PriorityQueue<BookItem> queue = null;
        Lock a_lock = null;
        Condition a_cond = null;

        public TravelBook(){
            a_lock = new ReentrantLock();
            a_cond = a_lock.newCondition();
            queue = new PriorityQueue<>();
        }

        public BookItem wait_for_new_element(){
            a_lock.lock();
            //a_lock.lockInterruptibly();
            try {
                try {
                    while (queue.size() == 0)
                        a_cond.await();
                } catch (InterruptedException ie){
                    a_cond.signal();
                }
                BookItem x = queue.poll();
                assert x != null;
                return x;
            } finally {
                a_lock.unlock();
            }
        }

        public void add_element(BookItem bi){
            a_lock.lock();
            try {
                boolean flag = false;
                for (BookItem b : queue) {
                    if (b.tid > bi.tid || b.sid > bi.sid)
                        break;

                    if (b.tid == bi.tid && b.sid == bi.sid) {
                        b.add_uuids(bi.uuids);
                        b.add_keys(bi.keys);
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    queue.offer(bi);
                }
                a_cond.signal();
            } finally {
                a_lock.unlock();
            }
        }
    }
    private class lock_and_wait{
        Lock a_lock;
        Condition a_cond;
        boolean finished;

        public lock_and_wait(){
            a_lock = new ReentrantLock();
            a_cond = a_lock.newCondition();
            this.finished = false;
        }
        public void wait_until_finish(){
            a_lock.lock();
            while (finished == false) {
                try {
                    a_cond.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    a_lock.unlock();
                }
            }
        }
        public void finish(){
            a_lock.lock();
            if (finished == false){
                finished = true;
                a_cond.signal();
            }
            a_lock.unlock();
        }
    }
    private class BookItem implements Comparable<BookItem>{
        Set<ByteBuffer> keys;
        long tid;
        int sid, master_id;
        Set<Long> uuids;
        public BookItem(long tid, int sid, long uuid, Set<ByteBuffer> keys, int master_id){
            this.tid = tid;
            this.sid = sid;
            this.uuids = new HashSet<>();
            this.uuids.add(uuid);
            this.keys = keys;
            this.master_id = master_id;
        }
        public void add_uuids(Set<Long> uuids){
            this.uuids.addAll(uuids);
        }
        public void add_keys(Set<ByteBuffer> k){
            this.keys.addAll(k);
        }
        @Override
        public int compareTo(BookItem o) {
            if (this.tid < o.tid) return -1;
            else if (this.tid == o.tid){
                if (this.sid < o.sid) return -1;
                else if (this.sid == o.sid) return 0;
                else return 1;
            }
            else return 1;
        }
    }

    public abfs(AbstractSrv srv){
        this.instance = srv;
        this.running_tasks = new HashMap<>();
        this.total_steps = new HashMap<>();
        this.travel_payloads = new HashMap<>();
        this.lock_to_travels = new ReentrantLock();
        this.locks = new HashMap<>();

        this.vertex_book = new TravelBook();
        this.edge_book = new TravelBook();

        this.pass_vertices_cache = new HashMap<>();
        this.sent_vertices_cache = new HashMap<>();

        for (int i = 0; i < 1; i++) {
            this.instance.workerPool.execute(new check_vertex_book());
            this.instance.workerPool.execute(new check_edge_book());
        }
    }

    /* code derived from javasnowflake (github) */
    private long gen_uuid(int machine_id){
        final long sequenceBits = 12;
        final long datacenterIdBits = 10L;
        final long datacenterIdShift = sequenceBits;
        final long timestampLeftShift = sequenceBits + datacenterIdBits;

        final long sequenceMax = 4096;
        final long twepoch = 1288834974657L;
        long sequence = 0L;

        long timestamp = System.currentTimeMillis();
        sequence = (sequence + 1) % sequenceMax;
        Long id = ((timestamp - twepoch) << timestampLeftShift) |
                (machine_id << datacenterIdShift) |
                sequence;
        return id;
    }


    public int async_travel_master(long tid, String payload){
        long abfs_start = System.currentTimeMillis();

        this.locks.put(tid, new lock_and_wait());
        this.travel_payloads.put(tid, payload);

        ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);
        this.total_steps.put(tid, travelPlan.size());
        int master_id = instance.getLocalIdx();
        int sid = 0;

        List<byte[]> keySet = travelPlan.get(sid).vertexKeyRestrict.values();
        HashMap<Integer, HashSet<ByteBuffer>> servers_store_keys_next_step = get_servers_from_keys_1(keySet);

        Set<Long> uuids = new HashSet<>();
        HashMap<Integer, Long> server_uuid_map = new HashMap<>();
        for (int s : servers_store_keys_next_step.keySet()){
            long uuid = gen_uuid(s);
            server_uuid_map.put(s, uuid);
            uuids.add(uuid);
        }
        rpc_async_travel_report(master_id, tid, sid, uuids, 0);

        for (int s : servers_store_keys_next_step.keySet()) {
            HashSet<ByteBuffer> keys_set = servers_store_keys_next_step.get(s);
            rpc_async_travel_vertices(s, tid, sid, server_uuid_map.get(s), keys_set, master_id, payload);
        }

        this.locks.get(tid).wait_until_finish();

        int travel_time = (int) (System.currentTimeMillis() - abfs_start);
        GLogger.info("abfs travel time: %d", travel_time);
        return travel_time;
    }

    private class check_vertex_book implements Runnable{
        @Override
        public void run() {
            while (true){      
                BookItem bi = vertex_book.wait_for_new_element();
                long tid = bi.tid;
                int sid = bi.sid;
                Set<ByteBuffer> keys_param = bi.keys;
                int master_id = bi.master_id;
                String payload = travel_payloads.get(tid);

                GLogger.info("Scan Vertex BookItem on StepId[%d]", sid);

                // old code
                ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);

                if (sid >= travelPlan.size()) { // reach the last step, stop to access its edges
                    rpc_async_travel_report(master_id, tid, sid, bi.uuids, 1);
                    continue;
                }

                SingleStep currStep = travelPlan.get(sid);
                HashSet<ByteBuffer> keys = new HashSet<>(keys_param);
                int before_checking_cache = keys.size();

                //before actually accessing the disk data, try to look at the cache first.
                if (!pass_vertices_cache.containsKey(sid))
                    pass_vertices_cache.put(sid, new HashSet<ByteBuffer>());
                HashSet<ByteBuffer> cached_keys = pass_vertices_cache.get(sid);
                keys.removeAll(cached_keys);  //we do not check cached vertices anymore
                GLogger.info("Step[%d] read cached data: %d", sid, (before_checking_cache - keys.size()));

                ArrayList<byte[]> passedVertices =
                        TravelLocalReader.filterVertices(instance.localStore, keys, currStep,0);

                //if the vertices has been processed, we do not need to further traversal from it.

                HashMap<Integer, HashSet<ByteBuffer>> edges_and_servers = new HashMap<>();
                for (byte[] v : passedVertices) {
                    Set<Integer> srvs = instance.getEdgeLocs(v);
                    for (int s : srvs) {
                        if (!edges_and_servers.containsKey(s))
                            edges_and_servers.put(s, new HashSet<ByteBuffer>());
                        edges_and_servers.get(s).add(ByteBuffer.wrap(v));
                        cached_keys.add(ByteBuffer.wrap(v));
                    }
                }

                Set<Integer> edge_servers = new HashSet<>(edges_and_servers.keySet());

                Set<Long> uuids = new HashSet<>();
                HashMap<Integer, Long> server_uuid_map = new HashMap<>();
                for (int s : edge_servers) {
                    long uuid = gen_uuid(s);
                    server_uuid_map.put(s, uuid);
                    uuids.add(uuid);
                }
                rpc_async_travel_report(master_id, tid, sid, uuids, 0);
                for (int s : edge_servers) {
                    HashSet<ByteBuffer> keys_set = edges_and_servers.get(s);
                    rpc_async_travel_edges(s, tid, sid, server_uuid_map.get(s), keys_set, master_id, payload);
                }
                rpc_async_travel_report(master_id, tid, sid, bi.uuids, 1);
            }
        }
    }

    public int async_travel_vertices(long tid, int sid, Set<ByteBuffer> keys_param,
                                     long uuid, int master_id, String payload){

        if (!this.travel_payloads.containsKey(tid))
            this.travel_payloads.put(tid, payload);
        BookItem bi = new BookItem(tid, sid, uuid, keys_param, master_id);
        this.vertex_book.add_element(bi);
        return 0;
    }

    private class check_edge_book implements Runnable{
        @Override
        public void run() {
            while (true){
                BookItem bi = edge_book.wait_for_new_element();
                long tid = bi.tid;
                int sid = bi.sid;
                Set<ByteBuffer> keys = bi.keys;
                int master_id = bi.master_id;
                String payload = travel_payloads.get(tid);

                GLogger.info("Scan Edge BookItem on Step[%d]", sid);

                ArrayList<SingleStep> travelPlan = build_travel_plan_from_json_string(payload);

                if (sid >= travelPlan.size())
                    continue;

                SingleStep currStep = travelPlan.get(sid);

                ArrayList<byte[]> passedVertices = new ArrayList<>();
                for (ByteBuffer k : keys)
                    passedVertices.add(NIOHelper.getActiveArray(k));

                HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(
                        instance.localStore, passedVertices, currStep, 0);

                int before_cache = nextVertices.size();

                // sent_vertices_cache: if we already sent out requests for v1 in step i, we do not do it again.
                if (!sent_vertices_cache.containsKey(sid))
                    sent_vertices_cache.put(sid, new HashSet<ByteBuffer>());
                HashSet<ByteBuffer> vertices_already_sent = sent_vertices_cache.get(sid);

                HashSet<byte[]> next_vertices_after_cache = new HashSet<byte[]>();
                for (byte[] v : nextVertices){
                    ByteBuffer k = ByteBuffer.wrap(v);
                    if (vertices_already_sent.contains(k))
                        continue;
                    next_vertices_after_cache.add(v);
                }

                GLogger.info("Step[%d] reduces sent %d vertices using cache",
                        sid, (before_cache - next_vertices_after_cache.size()));

                HashMap<Integer, HashSet<ByteBuffer>> vertices_and_servers = new HashMap<>();
                for (byte[] v : next_vertices_after_cache) {
                    Set<Integer> srvs = instance.getEdgeLocs(v);
                    for (int s : srvs) {
                        if (!vertices_and_servers.containsKey(s))
                            vertices_and_servers.put(s, new HashSet<ByteBuffer>());
                        vertices_and_servers.get(s).add(ByteBuffer.wrap(v));
                        vertices_already_sent.add(ByteBuffer.wrap(v));
                    }
                }
                Set<Integer> vertex_servers = new HashSet<>(vertices_and_servers.keySet());

                Set<Long> uuids = new HashSet<>();
                HashMap<Integer, Long> server_uuid_map = new HashMap<>();
                for (int s : vertex_servers) {
                    long uuid = gen_uuid(s);
                    server_uuid_map.put(s, uuid);
                    uuids.add(uuid);
                }
                rpc_async_travel_report(master_id, tid, sid + 1, uuids, 0);

                for (int s : vertex_servers) {
                    HashSet<ByteBuffer> keys_set = vertices_and_servers.get(s);
                    rpc_async_travel_vertices(s, tid, sid + 1, server_uuid_map.get(s), keys_set, master_id, payload);
                }
                rpc_async_travel_report(master_id, tid, sid, bi.uuids, 1);

            }
        }
    }

    public int async_travel_edges(long tid, int sid, Set<ByteBuffer> keys,
                                  long uuid, int master_id, String payload){
        if (!this.travel_payloads.containsKey(tid))
            this.travel_payloads.put(tid, payload);
        BookItem bi = new BookItem(tid, sid, uuid, keys, master_id);
        this.edge_book.add_element(bi);
        return 0;
    }

    public int async_travel_report(long tid, int sid, Set<Long> uuids, int type){
        this.lock_to_travels.lock();

        if (type == 0){  //new task is created
            if (!running_tasks.containsKey(tid))
                running_tasks.put(tid, new HashSet<Long>());
            running_tasks.get(tid).addAll(uuids);
        } else {
            running_tasks.get(tid).removeAll(uuids);
            if (running_tasks.get(tid).isEmpty() && (sid == this.total_steps.get(tid))){
                this.locks.get(tid).finish();
            }
        }

        this.lock_to_travels.unlock();
        return 0;
    }

    private void rpc_async_travel_vertices(int server_id, long tid, int sid, long uuid,
                                          HashSet<ByteBuffer> keys, int master_id, String payload){
        TGraphFSServer.Client client = null;
        try {
            client = instance.getClientConn(server_id);
            client.async_travel_vertices(tid, sid, keys, uuid, master_id, payload);
            instance.releaseClientConn(server_id, client);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void rpc_async_travel_edges(int server_id, long tid, int sid, long uuid,
                                       HashSet<ByteBuffer> keys, int master_id, String payload){
        TGraphFSServer.Client client = null;
        try {
            client = instance.getClientConn(server_id);
            client.async_travel_edges(tid, sid, keys, uuid, master_id, payload);
            instance.releaseClientConn(server_id, client);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void rpc_async_travel_report(int server_id, long tid, int sid, Set<Long> uuids, int type){
        TGraphFSServer.Client client = null;
        try {
            client = instance.getClientConn(server_id);
            client.async_travel_report(tid, sid, uuids, type);
            instance.releaseClientConn(server_id, client);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    // Helper functions
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
}
