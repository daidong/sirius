package edu.dair.sgdb.gclient.iogp;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.tengine.travel.GTravel;
import edu.dair.sgdb.gclient.GraphClt;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.RedirectException;
import edu.dair.sgdb.thrift.Status;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.NIOHelper;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IOGPClt extends GraphClt {

    HashMap<ByteBuffer, Integer> cachedLocationInfo = new HashMap<>();
    HashMap<ByteBuffer, Integer> cachedSplitInfo = new HashMap<>();
    int reassign_times = 0;

    public IOGPClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    private int getSplitInfoFromCache(byte[] src) {
        if (cachedSplitInfo.containsKey(ByteBuffer.wrap(src)))
            return cachedSplitInfo.get(ByteBuffer.wrap(src));
        else return 0;
    }

    private int getLocationFromCache(byte[] src) {
        if (cachedLocationInfo.containsKey(ByteBuffer.wrap(src)))
            return cachedLocationInfo.get(ByteBuffer.wrap(src));
        else
            return getHashLocation(src, this.serverNum);
    }

    private void updateLocationToCache(byte[] src, int target) {
        cachedLocationInfo.put(ByteBuffer.wrap(src), target);
    }

    private void invaidLocationCache(byte[] src) {
        cachedLocationInfo.remove(ByteBuffer.wrap(src));
    }

    public List<KeyValue> read(byte[] srcVertex,
                               EdgeType edgeType,
                               byte[] dstKey) throws TException {

        int target = getLocationFromCache(srcVertex);
        int retry = 0;
        boolean split = false;

        while (true) {
            try {
                List<KeyValue> rtn = getClientConn(target).read(ByteBuffer.wrap(srcVertex),
                        ByteBuffer.wrap(dstKey),
                        edgeType.get());
                updateLocationToCache(srcVertex, target);
                return rtn;

            } catch (RedirectException re) {
                invaidLocationCache(srcVertex);

                int status = re.getStatus();
                if (status == Constants.RE_ACTUAL_LOC)
                    target = re.getTarget();
                else if (status == Constants.RE_VERTEX_WRONG_SRV)
                    target = getHashLocation(srcVertex, this.serverNum);
                else if (status == Constants.EDGE_SPLIT_WRONG_SRV && split == false) {
                    split = true;
                    target = getLocationFromCache(dstKey);
                } else if (status == Constants.EDGE_SPLIT_WRONG_SRV && split == true)
                    target = getHashLocation(dstKey, this.serverNum);
            }
            if (retry++ > Constants.RETRY)
                break;
        }
        return (new ArrayList<KeyValue>());
    }


    public int insert(byte[] srcVertex,
                      EdgeType edgeType,
                      byte[] dstKey,
                      byte[] value) throws TException {

        int target = getLocationFromCache(srcVertex);
        int retry = 0;
        boolean split = false;

        while (true) {
            try {
                getClientConn(target).insert(ByteBuffer.wrap(srcVertex),
                        ByteBuffer.wrap(dstKey),
                        edgeType.get(),
                        ByteBuffer.wrap(value));
                break;

            } catch (RedirectException re) {
                int status = re.getStatus();
                invaidLocationCache(srcVertex);

                if (status == Constants.RE_ACTUAL_LOC) {
                    reassign_times++;
                    System.out.println("reassign: " + reassign_times);
                    target = re.getTarget();
                } else if (status == Constants.RE_VERTEX_WRONG_SRV) {
                    target = getHashLocation(srcVertex, this.serverNum);
                } else if (status == Constants.EDGE_SPLIT_WRONG_SRV && split == false) {
                    split = true;
                    cachedSplitInfo.put(ByteBuffer.wrap(srcVertex), 1);
                    target = getLocationFromCache(dstKey);
                } else if (status == Constants.EDGE_SPLIT_WRONG_SRV && split == true) {
                    target = getHashLocation(dstKey, this.serverNum);
                    cachedSplitInfo.put(ByteBuffer.wrap(srcVertex), 1);
                }
            }
            if (retry++ > Constants.RETRY)
                break;
        }
        updateLocationToCache(srcVertex, target);
        return Constants.RTN_SUCC;
    }

    public int syncstatus() throws TException {

        ArrayList<Status> statuses = new ArrayList<>();
        HashSet<ByteBuffer> touchedVertices = new HashSet<>();
        touchedVertices.addAll(cachedLocationInfo.keySet());
        touchedVertices.addAll(cachedSplitInfo.keySet());

        for (ByteBuffer v : touchedVertices) {
            Status s = new Status();
            s.setKey(NIOHelper.getActiveArray(v));
            s.setLocation(getLocationFromCache(NIOHelper.getActiveArray(v)));
            s.setIssplit(getSplitInfoFromCache(NIOHelper.getActiveArray(v)));
            statuses.add(s);
        }

        final Lock lock = new ReentrantLock();
        AtomicInteger total_broadcast = new AtomicInteger(serverNum);
        final Condition broadcast_finish = lock.newCondition();

        try {
            for (int i = 0; i < serverNum; i++) {
                TGraphFSServer.AsyncClient aclient = getAsyncClientConn(i);
                AsyncMethodCallback amcb = new SyncAllCallback(lock, broadcast_finish, total_broadcast);
                aclient.syncstatus(statuses, amcb);
            }

            lock.lock();
            try {
                broadcast_finish.await();
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return 0;
    }

    class SyncAllCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.syncstatus_call> {

        public Lock lock;
        public AtomicInteger total_broadcast;
        public Condition broadcast_finish;

        public SyncAllCallback(Lock l, Condition f, AtomicInteger total) {
            lock = l;
            total_broadcast = total;
            broadcast_finish = f;
        }

        @Override
        public void onComplete(TGraphFSServer.AsyncClient.syncstatus_call t) {
            lock.lock();
            try {
                if (total_broadcast.decrementAndGet() == 0)
                    broadcast_finish.signal();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void onError(Exception e) {
        }
    }

    public List<ByteBuffer> bfs(byte[] srcVertex, EdgeType edgeType, int max_steps) throws TException {
        HashSet<ByteBuffer> visited = new HashSet<>();
        return new ArrayList<>(visited);
    }

    public List<KeyValue> scan(byte[] srcVertex,
                               EdgeType edgeType) throws TException {

        int target = getLocationFromCache(srcVertex);
        int retry = 0;
        boolean split = false;

        while (!split) {
            try {
                TGraphFSServer.Client client = getClientConn(target);
                List<KeyValue> rtn = client.scan(ByteBuffer.wrap(srcVertex), edgeType.get());
                updateLocationToCache(srcVertex, target);
                return rtn;
            } catch (RedirectException re) {
                invaidLocationCache(srcVertex);

                int status = re.getStatus();
                if (status == Constants.RE_ACTUAL_LOC)
                    target = re.getTarget();
                else if (status == Constants.RE_VERTEX_WRONG_SRV)
                    target = getHashLocation(srcVertex, this.serverNum);
                else if (status == Constants.EDGE_SPLIT_WRONG_SRV)
                    split = true;
            }
            if (retry++ > Constants.RETRY)
                break;
        }

        if (split) {
            final Lock lock = new ReentrantLock();
            AtomicInteger total_broadcast = new AtomicInteger(serverNum);
            final Condition broadcast_finish = lock.newCondition();
            List<KeyValue> rtn = new ArrayList<>();

            try {
                for (int i = 0; i < serverNum; i++) {
                    TGraphFSServer.AsyncClient aclient = getAsyncClientConn(i);
                    AsyncMethodCallback amcb = new ScanAllCallback(lock, broadcast_finish, total_broadcast, rtn);
                    aclient.force_scan(ByteBuffer.wrap(srcVertex),
                            edgeType.get(),
                            amcb);
                }

                lock.lock();
                try {
                    broadcast_finish.await();
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return rtn;
        }
        return new ArrayList<KeyValue>();
    }

    class ScanAllCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.force_scan_call> {

        public Lock lock;
        public AtomicInteger total_broadcast;
        public Condition broadcast_finish;
        List<KeyValue> rtn;

        public ScanAllCallback(Lock l, Condition f, AtomicInteger total, List<KeyValue> kv) {
            lock = l;
            total_broadcast = total;
            broadcast_finish = f;
            rtn = kv;
        }

        @Override
        public void onComplete(TGraphFSServer.AsyncClient.force_scan_call t) {
            lock.lock();
            try {
                List<KeyValue> force_scan_rtn = t.getResult();
                /*
                for (KeyValue kv : force_scan_rtn){
                    DBKey k = new DBKey(kv.getKey());
                    logger.info("In scan callback: " + new String(k.src)
                            + " -> " + new String(k.dst));
                }
                */
                rtn.addAll(force_scan_rtn);
                if (total_broadcast.decrementAndGet() == 0)
                    broadcast_finish.signal();
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void onError(Exception e) {

        }
    }

    public static void main(String[] args) throws TException, IOException {
        int port = Integer.parseInt(args[0]);
        int index = Integer.parseInt(args[1]);
        int testType = Integer.parseInt(args[2]);
        String graphDir = args[3];

        ArrayList<String> allSrvs = new ArrayList<>();
        for (int i = 4; i < args.length; i++) {
            allSrvs.add(args[i]);
        }

        IOGPClt vcc = new IOGPClt(port, allSrvs);

        if (testType == 0) { //insert test
            String graphFile = graphDir + "/" + index;
            ArrayList<String> inMemoryGraph = new ArrayList<String>();
            try (BufferedReader br = new BufferedReader(new FileReader(graphFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    inMemoryGraph.add(line);
                }
            }
            long start = System.currentTimeMillis();
            for (String line : inMemoryGraph) {
                String[] splits = line.split(" ");
                byte[] src = splits[0].getBytes();
                byte[] dst = splits[1].getBytes();
                byte[] val = splits[2].getBytes();
                vcc.insert(src, EdgeType.IN, dst, val);
            }
            System.out.println("Insert time: " + (System.currentTimeMillis() - start));
        }

        if (testType == 3) {
            long start = 0;
            /*
             int totalSize = 1024;
             String payload256 = "";
             for (int i = 0; i < 128; i++){
             payload256 += "a";
             }
             byte[] vals = payload256.getBytes();
             start = System.currentTimeMillis();
			
			
             for (int i = index * totalSize; i < (index + 1) * totalSize; i ++){
             try {
             vcc.insert(
             ("vertex"+(2000000)).getBytes(), 
             EdgeType.RUN, 
             ("vertex"+(i)).getBytes(), 
             vals);
             } catch (TException e1) {
             e1.printStackTrace();
             }
             }
             System.out.println("Insert " + ("vertex"+(2000000)) + " time: " + (System.currentTimeMillis() - start));
             */

            String graphFile = graphDir + "/" + index;
            ArrayList<String> inMemoryGraph = new ArrayList<String>();
            try (BufferedReader br = new BufferedReader(new FileReader(graphFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    inMemoryGraph.add(line);
                }
            }
            start = System.currentTimeMillis();
            //int times = 0;
            for (String line : inMemoryGraph) {
                String[] splits = line.split(" ");
                byte[] src = splits[0].getBytes();
                byte[] dst = splits[1].getBytes();
                byte[] val = splits[2].getBytes();
                vcc.insert(src, EdgeType.IN, dst, val);
            }
            System.out.println("Insert all graph time: " + (System.currentTimeMillis() - start));
            /*
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("vertex"+(2000000)).getBytes(), EdgeType.RUN);
             System.out.println("["+index+"] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
			
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("vertex"+(2000000)).getBytes(), EdgeType.RUN);
             System.out.println("["+index+"] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
			
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("vertex"+(2000000)).getBytes(), EdgeType.RUN);
             System.out.println("["+index+"] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
			
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("1000301241:1488").getBytes(), EdgeType.RUN);
             System.out.println("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
			
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("398420835:0").getBytes(), EdgeType.RUN);
             System.out.println("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
			
             if (index == 0){
             start = System.currentTimeMillis();
             List<KeyValue> r = vcc.scan(("2610467296").getBytes(), EdgeType.RUN);
             System.out.println("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
             }
             */
        }
        if (testType == 1) { //2-step sync traversal.

            GTravel gt = new GTravel();
            int edge = EdgeType.IN.get();
            byte[] bEdge = ArrayPrimitives.itob(edge);

            gt.v(("vertex0").getBytes())
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .v();
            vcc.submitSyncTravel(gt.plan());

        }

        if (testType == 2) {

            GTravel gt = new GTravel();
            int edge = EdgeType.IN.get();
            byte[] bEdge = ArrayPrimitives.itob(edge);

            gt.v(("vertex0").getBytes())
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .et(bEdge).next()
                    .v();
            vcc.submitTravel(gt.plan());
        }
    }
}
