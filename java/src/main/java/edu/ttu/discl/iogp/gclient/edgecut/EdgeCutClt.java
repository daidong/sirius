package edu.ttu.discl.iogp.gclient.edgecut;

import edu.ttu.discl.iogp.gclient.Batch;
import edu.ttu.discl.iogp.gclient.GraphClt;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.tengine.travel.GTravel;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.utils.ArrayPrimitives;
import edu.ttu.discl.iogp.utils.JenkinsHash;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EdgeCutClt extends GraphClt {

    private static final Logger logger = LoggerFactory.getLogger(EdgeCutClt.class);

    public EdgeCutClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        long ts = System.currentTimeMillis();
        return read(srcVertex, edgeType, dstKey, ts);
    }

    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, long ts) throws TException {
        int dstServer = getEdgeLocation(srcVertex, this.serverNum);
        List<KeyValue> r = getClientConn(dstServer).read(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get());
        return r;
    }

    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        long ts = System.currentTimeMillis();
        return insert(srcVertex, edgeType, dstKey, value, ts);
    }

    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value, long ts) throws TException {
        int dstServer = getEdgeLocation(srcVertex, this.serverNum);
        int r = getClientConn(dstServer).insert(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get(), ByteBuffer.wrap(value));
        return r;
    }

    @Override
    public int batch_insert(Batch b) {
        ArrayList<Thread> threads = new ArrayList<Thread>();

        for (ByteBuffer key : b.keySet()) {
            byte[] srcVertex = NIOHelper.getActiveArray(key);
            int dstServer = getEdgeLocation(srcVertex, this.serverNum);
            List<KeyValue> data = b.getEntries(key);

            Thread t = new Thread(new BatchInsertionReceiver(dstServer, data));
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

        public BatchInsertionReceiver(int sid, List<KeyValue> d) {
            this.server = sid;
            this.data = d;
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
        int dstServer = getEdgeLocation(srcVertex, this.serverNum);
        List<KeyValue> r = getClientConn(dstServer).scan(ByteBuffer.wrap(srcVertex), edgeType.get());
        return r;
    }

    @Override
    public HashMap<Integer, Integer> getStats() {
        return null;
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

        EdgeCutClt ecc = new EdgeCutClt(port, allSrvs);

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
                ecc.insert(src, EdgeType.IN, dst, val);
            }
            logger.info("Insert time: " + (System.currentTimeMillis() - start));
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
             ecc.insert(
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
				//if (times >= 10000)
                //	break;
                //times++;
                String[] splits = line.split(" ");
                byte[] src = splits[0].getBytes();
                byte[] dst = splits[1].getBytes();
                byte[] val = splits[2].getBytes();
                ecc.insert(src, EdgeType.IN, dst, val);
            }
            logger.info("Insert all graph time: " + (System.currentTimeMillis() - start));

            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("vertex" + (2000000)).getBytes(), EdgeType.IN);
                logger.info("[" + index + "] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }

            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("vertex" + (2000000)).getBytes(), EdgeType.IN);
                logger.info("[" + index + "] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }

            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("vertex" + (2000000)).getBytes(), EdgeType.IN);
                logger.info("[" + index + "] Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }

            //1, 12, 2560
            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("1000301241:1488").getBytes(), EdgeType.IN);
                logger.info("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }

            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("398420835:0").getBytes(), EdgeType.IN);
                logger.info("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }

            if (index == 0) {
                start = System.currentTimeMillis();
                List<KeyValue> r = ecc.scan(("2610467296").getBytes(), EdgeType.IN);
                logger.info("Scan time: " + (System.currentTimeMillis() - start) + " " + r.size() + " elements.");
            }
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
            ecc.submitSyncTravel(gt.plan());

        }

        if (testType == 2) {

            GTravel gt = new GTravel();
            int edge = EdgeType.IN.get();
            byte[] bEdge = ArrayPrimitives.itob(edge);

            gt.v(("vertex0").getBytes())
                    .et(bEdge).next()
                    .et(bEdge).next()
                    //.et(bEdge).next()
                    //.et(bEdge).next()
                    .v();
            ecc.submitTravel(gt.plan());
        }
    }
}
