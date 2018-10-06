package edu.dair.sgdb.gserver.iogp;

import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.Movement;
import edu.dair.sgdb.thrift.RedirectException;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IOGPGraphSplitMover {


    public class Task {
        int type;
        Object payload;

        public Task(ArrayList<KeyValue> payload) {
            this.type = 1;
            this.payload = (Object) payload;
        }
    }

    public IOGPSrv inst;
    LinkedBlockingQueue<Task> taskQueues[];
    final Lock lock = new ReentrantLock();
    ConcurrentHashMap<ByteBuffer, AtomicInteger> broadcasts;
    ConcurrentHashMap<ByteBuffer, Condition> broadcast_finishes;

    public IOGPGraphSplitMover(IOGPSrv instance) {
        this.inst = instance;
        taskQueues = new LinkedBlockingQueue[inst.serverNum];
        broadcasts = new ConcurrentHashMap<>();
        broadcast_finishes = new ConcurrentHashMap<>();
    }

    public void startWorkers() {
        for (int i = 0; i < inst.serverNum; i++) {
            taskQueues[i] = new LinkedBlockingQueue<>();
            new Thread(new Worker(i, taskQueues[i], inst)).start();
        }
    }

    public void splitVertex(ByteBuffer src) throws InterruptedException {
        byte[] bsrc = NIOHelper.getActiveArray(src);
        boolean jump = false;

        broadcasts.putIfAbsent(src, new AtomicInteger(inst.serverNum));
        AtomicInteger broadcast = broadcasts.get(src);
        broadcast_finishes.putIfAbsent(src, lock.newCondition());
        final Condition broadcast_finish = broadcast_finishes.get(src);

        for (int i = 0; i < inst.serverNum; i++) {
            try {
                TGraphFSServer.Client client = inst.getClientConn(i);
                if (i != inst.getLocalIdx()) {
                    client.iogp_split(src);
                    if (broadcast.decrementAndGet() == 0) {
                        broadcast_finish.signal();
                    }
                }
                else {
                    inst.handler.iogp_split(src);
                    if (broadcast.decrementAndGet() == 0) {
                        jump = true;
                    }
                }
                inst.releaseClientConn(i, client);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        if (!jump) {
            lock.lock();
            try {
                broadcast_finish.await();
            } finally {
                lock.unlock();
            }
        }

        GLogger.info("[%d] broadcast and get split done from all servers", inst.getLocalIdx());
        broadcasts.remove(src);
        broadcast_finishes.remove(src);

        DBKey startKey = DBKey.MinDBKey(bsrc);
        DBKey endKey = DBKey.MaxDBKey(bsrc);
        ArrayList<KeyValue> kvs = inst.localStore.scanKV(startKey.toKey(), endKey.toKey());
        HashMap<Integer, ArrayList<KeyValue>> loadsOnEachServer = new HashMap<>();

        for (KeyValue kv : kvs) {
            DBKey key = new DBKey(kv.getKey());
            int hash_target = inst.getHashLocation(key.dst, inst.serverNum);
            if (!loadsOnEachServer.containsKey(hash_target))
                loadsOnEachServer.put(hash_target, new ArrayList<KeyValue>());
            loadsOnEachServer.get(hash_target).add(kv);
        }
        for (int target : loadsOnEachServer.keySet()) {
            addDataMovementTask(target, loadsOnEachServer.get(target));
        }
    }

    public void addDataMovementTask(int i, ArrayList<KeyValue> kv) throws InterruptedException {
        if (i != inst.getLocalIdx()) {
            Task t = new Task(kv);
            this.taskQueues[i].put(t);
        }
    }

    class Worker implements Runnable {
        public int target;
        LinkedBlockingQueue<Task> tasks;
        IOGPSrv inst;

        public Worker(int t, LinkedBlockingQueue<Task> q, IOGPSrv inst) {
            this.target = t;
            this.tasks = q;
            this.inst = inst;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Task task = tasks.take();

                    if (task.type == 1) { //move data
                        ArrayList<KeyValue> kvs = (ArrayList<KeyValue>) task.payload;
                        List<Movement> do_it_again = new ArrayList<>();

                        if (kvs != null) {
                            try {
                                TGraphFSServer.Client client = inst.getClientConn(this.target);
                                synchronized (client) {
                                    client.batch_insert(kvs, 0);
                                }
                            } catch (RedirectException e) {

                                do_it_again = e.getRe();
                                for (Movement m : do_it_again) {
                                    ArrayList<KeyValue> payload = new ArrayList<>();
                                    payload.add(m.getKv());

                                    try {
                                        inst.spliter.addDataMovementTask(m.getLoc(), payload);
                                    } catch (InterruptedException e1) {
                                        e1.printStackTrace();
                                    }
                                }

                            } catch (TException e) {
                                e.printStackTrace();
                            } finally {
                                int before_kvs = kvs.size();
                                inst.size.addAndGet(do_it_again.size() - kvs.size());
                                for (Movement m : do_it_again) {
                                    kvs.remove(m.getKv());
                                }
                                if (kvs.size() != (before_kvs - do_it_again.size())) {
                                    GLogger.error("[ERROR], before size: %d, do_it_again size:%d" +
                                                    " after size: %d", before_kvs, do_it_again.size(),
                                            kvs.size());
                                }

                                /**
                                 * Remove local copy
                                 */
                                for (KeyValue kv : kvs) {
                                    inst.localStore.remove(kv.getKey());
                                }
                            }
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
