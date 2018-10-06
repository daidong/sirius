package edu.dair.sgdb.gserver.iogp;

import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reassigner take src as input and take charge all reassignments.
 */
public class IOGPGraphReassigner {


    public IOGPSrv inst;
    final Lock lock = new ReentrantLock();
    ConcurrentHashMap<ByteBuffer, AtomicInteger> broadcasts;
    ConcurrentHashMap<ByteBuffer, Condition> broadcast_finishes;
    int fennel_score[];


    public IOGPGraphReassigner(IOGPSrv srv) {
        inst = srv;
        broadcasts = new ConcurrentHashMap<>();
        broadcast_finishes = new ConcurrentHashMap<>();
        fennel_score = new int[inst.serverNum];
        for (int i = 0; i < inst.serverNum; i++) fennel_score[i] = 0;
    }

    class JMP {
        public boolean v;
    }

    public void reassignVertex(ByteBuffer src) throws InterruptedException {

        /**
         * First Step: Collect all edge counters
         */
        byte[] bsrc = NIOHelper.getActiveArray(src);

        JMP jmp = new JMP();
        jmp.v = false;

        broadcasts.putIfAbsent(src, new AtomicInteger(inst.serverNum));
        AtomicInteger broadcast = broadcasts.get(src);
        broadcast_finishes.putIfAbsent(src, lock.newCondition());
        final Condition broadcast_finish = broadcast_finishes.get(src);

        long start = System.currentTimeMillis();

        for (int i = 0; i < inst.serverNum; i++) {
            try {
                TGraphFSServer.Client client = inst.getClientConn(i);
                if (i != inst.getLocalIdx()) {
                    fennel_score[i] = client.iogp_fennel(src);

                    lock.lock();
                    try {
                        if (broadcast.decrementAndGet() == 0) {
                            broadcast_finish.signal();
                            jmp.v = true;
                        }
                    } finally {
                        lock.unlock();
                    }

                } else {
                    lock.lock();
                    try {
                        fennel_score[i] = (0 - inst.size.get());
                        if (broadcast.decrementAndGet() == 0) {
                            jmp.v = true;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                inst.releaseClientConn(i, client);
            } catch (TException e) {
                e.printStackTrace();
            }

        }

        lock.lock();
        try {
            if (!jmp.v)
                broadcast_finish.await();
        } finally {
            lock.unlock();
        }

        GLogger.info("[%d] Get Fennel Score From %d Servers Cost: %d",
                inst.getLocalIdx(), inst.serverNum, System.currentTimeMillis() - start);

        broadcasts.remove(src);
        broadcast_finishes.remove(src);

        /**
         * Second Step: Choose the best one and decide whether move the vertices or not
         */
        inst.edgecounters.putIfAbsent(src, new Counters());
        Counters c = inst.edgecounters.get(src);

        boolean reassign_decision = false;
        int local_score = 2 * (c.pli + c.plo) - inst.size.get();
        int max_score = local_score;

        int target = inst.getLocalIdx();
        for (int i = 0; i < inst.serverNum; i++)
            if (fennel_score[i] > max_score) {
                max_score = fennel_score[i];
                target = i;
            }

        if (target != inst.getLocalIdx())
            if (max_score - local_score > 10)
                reassign_decision = true;

        /**
         * Third Step: Medata Update
         */
        if (reassign_decision != true) return;
        GLogger.info("[%d] %s should move from %d to %d for %d times",
                inst.getLocalIdx(), new String(bsrc),
                inst.getLocalIdx(), target, c.reassign_times);

        try {
            /**
             * The order is important. First setup target, then update the hash sorce
             */
            int hash_loc = inst.getHashLocation(bsrc, inst.serverNum);
            // to the target server, "type" is how many this vertex has been reassigned.
            if (c.reassign_times < 1) GLogger.error("c.reassign_times should never small than 1");

            TGraphFSServer.Client targetClient = inst.getClientConn(target);
            synchronized (targetClient) {
                targetClient.iogp_reassign(src, c.reassign_times, target);
            }
            inst.releaseClientConn(target, targetClient);

            TGraphFSServer.Client hashClient = inst.getClientConn(hash_loc);
            synchronized (hashClient) {
                if (hash_loc != inst.getLocalIdx())
                    hashClient.iogp_reassign(src, 0, target);
                else
                    inst.handler.iogp_reassign(src, 0, target);
            }
            inst.releaseClientConn(hash_loc, hashClient);

        } catch (TException e) {
            e.printStackTrace();
        }

        c.plo = c.alo;
        c.pli = c.ali;
        inst.loc.remove(src);

        DBKey startKey = DBKey.MinDBKey(bsrc);
        DBKey endKey = DBKey.MaxDBKey(bsrc);
        ArrayList<KeyValue> kvs = inst.localStore.scanKV(startKey.toKey(), endKey.toKey());
        inst.size.addAndGet(0 - kvs.size());

        for (KeyValue kv : kvs) {
            DBKey key = new DBKey(kv.getKey());
            ByteBuffer bdst = ByteBuffer.wrap(key.dst);
            if (!inst.edgecounters.containsKey(bdst))
                inst.edgecounters.put(bdst, new Counters());
            Counters dstc = inst.edgecounters.get(bdst);

            if (inst.loc.containsKey(bdst)
                    && inst.loc.get(bdst) == inst.getLocalIdx()) {
                if (key.type == EdgeType.OUT.get())
                    dstc.ali -= 1;
                if (key.type == EdgeType.IN.get())
                    dstc.alo -= 1;
            }
        }

        /**
         * Fourth Step: Conduct data movement
         */
        try {
            TGraphFSServer.Client client = inst.getClientConn(target);
            synchronized (client) {
                //GLogger.info("[%d]-[Reassign]-[%d] Client: %s calls batch_insert",
                //		inst.getLocalIdx(), target, client.toString());
                client.batch_insert(kvs, 1);
            }
            inst.releaseClientConn(target, client);

        } catch (TException e) {
            e.printStackTrace();
        }

        /**
         * Fifth Step: Delete local copy
         */
        for (KeyValue kv : kvs) {
            inst.localStore.remove(kv.getKey());
        }

    }
}
