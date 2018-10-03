package edu.dair.sgdb.gclient;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.tengine.travel.GTravel;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.NIOHelper;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class EdgeCutClt extends GraphClt {

    private static final Logger logger = LoggerFactory.getLogger(EdgeCutClt.class);

    public EdgeCutClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        int dstServer = getHashLocation(srcVertex, this.serverNum);
        return getClientConn(dstServer).read(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get());
    }

    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        int dstServer = getHashLocation(srcVertex, this.serverNum);
        return getClientConn(dstServer).insert(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get(), ByteBuffer.wrap(value));
    }

    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException {
        int dstServer = getHashLocation(srcVertex, this.serverNum);
        return getClientConn(dstServer).scan(ByteBuffer.wrap(srcVertex), edgeType.get());
    }

    private class BfsEntry {
        public ByteBuffer v;
        public int step;

        public BfsEntry(ByteBuffer b, int s) {
            v = b;
            step = s;
        }
    }

    public List<ByteBuffer> bfs(byte[] srcVertex, EdgeType edgeType, int max_steps) throws TException {
        HashSet<ByteBuffer> visited = new HashSet<>();
        LinkedBlockingQueue<BfsEntry> queue = new LinkedBlockingQueue<>();

        queue.offer(new BfsEntry(ByteBuffer.wrap(srcVertex), 0));

        BfsEntry current;

        while ((current = queue.poll()) != null) {
            byte[] key = NIOHelper.getActiveArray(current.v);
            int step = current.step;

            visited.add(current.v);

            if (step < max_steps) {

                List<KeyValue> results = scan(key, edgeType);

                for (KeyValue kv : results) {
                    DBKey newKey = new DBKey(kv.getKey());
                    byte[] dst = newKey.dst;
                    if (!visited.contains(ByteBuffer.wrap(dst)))
                        queue.offer(new BfsEntry(ByteBuffer.wrap(dst), step + 1));
                }
            }
        }

        return new ArrayList<>(visited);
    }

    @Override
    public int sync() throws TException {
        return 0;
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
