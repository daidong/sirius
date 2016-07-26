package edu.ttu.discl.iogp.gclient.iogp;

import edu.ttu.discl.iogp.gclient.Batch;
import edu.ttu.discl.iogp.gclient.GraphClt;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.tengine.travel.GTravel;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.ArrayPrimitives;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class IOGPClt extends GraphClt {

    private static final Logger logger = LoggerFactory.getLogger(IOGPClt.class);

    public IOGPClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        long ts = System.currentTimeMillis();
        return read(srcVertex, edgeType, dstKey, ts);
    }

    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, long ts) throws TException {
        //int dstServer = HashKeyLocation.getEdgeLoc(srcVertex, this.serverNum);
        int dstServer = getEdgeLocation(dstKey, this.serverNum);
        List<KeyValue> r = getClientConn(dstServer).read(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get());
        return r;
    }

    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        long ts = System.currentTimeMillis();
        return insert(srcVertex, edgeType, dstKey, value, ts);
    }

    private byte[] combine(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }

    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value, long ts) throws TException {
        int dstServer = getEdgeLocation(combine(srcVertex, dstKey), this.serverNum);
        int r = getClientConn(dstServer).insert(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get(), ByteBuffer.wrap(value));
        return r;
    }

    @Override
    public int batch_insert(Batch b) {
        ArrayList<Thread> threads = new ArrayList<Thread>();
        HashMap<Integer, ArrayList<KeyValue>> preServerData = new HashMap<>();

        for (ByteBuffer key : b.keySet()) {
            byte[] srcVertex = NIOHelper.getActiveArray(key);
            List<KeyValue> data = b.getEntries(key);
            for (KeyValue kv : data) {
                DBKey dbKey = new DBKey(kv.getKey());
                byte[] dstVertex = dbKey.dst;
                int dstServer = getEdgeLocation(combine(srcVertex, dstVertex), this.serverNum);
                if (!preServerData.containsKey(dstServer)) {
                    preServerData.put(dstServer, new ArrayList<KeyValue>());
                }
                preServerData.get(dstServer).add(kv);

            }
        }

        for (int s : preServerData.keySet()) {
            List<KeyValue> data = preServerData.get(s);
            Thread t = new Thread(new BatchInsertionReceiver(s, data));
            threads.add(t);
            t.start();

        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    private class BatchInsertionReceiver implements Runnable {

        int server;
        List<KeyValue> data;

        public BatchInsertionReceiver(int sid, List<KeyValue> data) {
            this.server = sid;
            this.data = data;
        }

        @Override
        public void run() {
            try {
                getClientConn(server).batch_insert(data, -1);
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException {
        long ts = System.currentTimeMillis();
        return scan(srcVertex, edgeType, ts);
    }

    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType, long ts) throws TException {
        return scan(srcVertex, edgeType, ts, ts);
    }

    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType, long start_ts, long end_ts) throws TException {
        ByteBuffer comparableKey = ByteBuffer.wrap(srcVertex);

        ArrayList<KeyValue> rtn = new ArrayList<>();
        List<KeyValue> synRtn = Collections.synchronizedList(rtn);
        HashSet<Integer> reqs = new HashSet<Integer>();

        for (int i = 0; i < this.serverNum; i++) {
            reqs.add(i);
        }

        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int server : reqs) {
            Thread t = new Thread(new AsyncReceiver(server, 
                    srcVertex, edgeType.get(), 
                    start_ts, end_ts, synRtn));
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return rtn;
    }

    @Override
    public HashMap<Integer, Integer> getStats() {
        return null;
    }

    private class AsyncReceiver implements Runnable {

        int server, type;
        List<KeyValue> receiver;
        byte[] srcVertex;
        long start, end;

        public AsyncReceiver(int sid, byte[] src, int type, long start, long end, List<KeyValue> receiver) {
            this.server = sid;
            this.srcVertex = src;
            this.type = type;
            this.start = start;
            this.end = end;
            this.receiver = receiver;
        }

        @Override
        public void run() {
            List<KeyValue> r = null;
            try {
                r = getClientConn(server).scan(ByteBuffer.wrap(srcVertex), type);
            } catch (TException e) {
                e.printStackTrace();
            }
            this.receiver.addAll(r);
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
