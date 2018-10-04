package edu.dair.sgdb.gclient;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.partitioner.DIDOIndex;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.JenkinsHash;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DIDOClt extends AbstractClt {
    public HashMap<ByteBuffer, DIDOIndex> gigaMaps;

    public DIDOClt(int port, ArrayList<String> alls) {
        super(port, alls);
        this.gigaMaps = new HashMap<>();
    }

    private DIDOIndex surelyGetGigaMap(byte[] bsrc) {
        ByteBuffer src = ByteBuffer.wrap(bsrc);
        if (!gigaMaps.containsKey(src)) {
            int startIdx = getHashLocation(bsrc, Constants.MAX_VIRTUAL_NODE);
            gigaMaps.put(src, new DIDOIndex(startIdx, this.serverNum));
        }
        return gigaMaps.get(src);
    }

    private int getServerLoc(byte[] src, byte[] dst) {
        DIDOIndex gi = surelyGetGigaMap(src);
        JenkinsHash jh = new JenkinsHash();
        int dstHash = Math.abs(jh.hash32(dst));
        int index = gi.giga_get_index_for_hash(dstHash);
        int server = gi.giga_get_server_from_index(index);
        return server;
    }

    @Override
    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        DIDOIndex gi = surelyGetGigaMap(srcVertex);

        while (true) {
            int target = getServerLoc(srcVertex, dstKey);
            try {
                List<KeyValue> r = this.getClientConn(target).read(ByteBuffer.wrap(srcVertex),
                        ByteBuffer.wrap(dstKey), edgeType.get());
                return r;
            } catch (RedirectException gre) {
                gi.giga_update_bitmap(gre.getBitmap());
            }
        }
    }

    @Override
    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        DIDOIndex gi = surelyGetGigaMap(srcVertex);
        int retry = 0;
        while (true) {
            int target = getServerLoc(srcVertex, dstKey);
            try {
                this.getClientConn(target).insert(ByteBuffer.wrap(srcVertex),
                        ByteBuffer.wrap(dstKey), edgeType.get(), ByteBuffer.wrap(value));
                return Constants.RTN_SUCC;
            } catch (RedirectException gre) {
                gi.giga_update_bitmap(gre.getBitmap());
            }
            retry++;
            if (retry >= Constants.RETRY) {
                Scanner s = new Scanner(System.in);
                s.next();
            }
        }
    }

    private HashSet<Integer> getAllSrvs() {
        HashSet<Integer> srvs = new HashSet<>();
        for (int i = 0; i < this.serverNum; i++) {
            srvs.add(i);
        }
        return srvs;
    }

    public HashMap<Integer, Integer> getStats() {
        HashMap<Integer, Integer> stats = new HashMap<>();
        HashSet<Integer> reqs = getAllSrvs();
        for (int server : reqs) {
            try {
                List<Dist> r = this.getClientConn(server).get_state();
                for (Dist d : r) {
                    int split = d.getSplitNum();
                    int vertex = d.getVertexNum();
                    if (!stats.containsKey(split)) {
                        stats.put(split, 0);
                    }
                    int v = stats.get(split);
                    stats.put(split, (vertex + v));
                }
            } catch (TException e) {
                e.printStackTrace();
            }
        }
        return stats;
    }

    private HashSet<Integer> getLocs(byte[] src, HashSet<Integer> excludes) {
        DIDOIndex gi = surelyGetGigaMap(src);
        HashSet<Integer> locs = gi.giga_get_all_servers();
        locs.removeAll(excludes);
        return locs;
    }

    private class ScanCallBack implements AsyncMethodCallback<TGraphFSServer.AsyncClient.giga_scan_call> {

        List<KeyValue> rtn;
        DIDOIndex gi;
        AtomicInteger ai;

        public ScanCallBack(DIDOIndex gi, AtomicInteger total, List<KeyValue> kvs) {
            this.rtn = kvs;
            this.gi = gi;
            this.ai = total;
        }

        @Override
        public void onComplete(TGraphFSServer.AsyncClient.giga_scan_call scan_call) {
            try {
                GigaScan scan_rtn = scan_call.getResult();
                this.rtn.addAll(scan_rtn.getKvs());
                gi.giga_update_bitmap(scan_rtn.getBitmap());
                this.ai.getAndDecrement();
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onError(Exception e) {
        }
    }

    @Override
    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException {
        DIDOIndex gi = surelyGetGigaMap(srcVertex);
        List<KeyValue> rtn = new ArrayList<>();
        HashSet<Integer> reqSrvs;
        HashSet<Integer> alreadySentSrvs = new HashSet<>();
        AtomicInteger totalReqs = new AtomicInteger(0);

        while (true) {
            reqSrvs = getLocs(srcVertex, alreadySentSrvs);
            alreadySentSrvs.addAll(reqSrvs);
            if (reqSrvs.isEmpty() && totalReqs.get() == 0)
                return new ArrayList<KeyValue>(rtn);

            AsyncMethodCallback amcb = new ScanCallBack(gi, totalReqs, rtn);
            for (int server : reqSrvs) {
                totalReqs.getAndIncrement();
                getAsyncClientConn(server).giga_scan(ByteBuffer.wrap(srcVertex), edgeType.get(), ByteBuffer.wrap(gi.bitmap), amcb);
            }
        }
    }

    @Override
    public List<ByteBuffer> bfs(byte[] srcVertex, EdgeType edgeType, int max_steps) throws TException {
        return null;
    }

    @Override
    public int sync() throws TException {
        return 0;
    }
}
