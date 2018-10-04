package edu.dair.sgdb.gserver.dido;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.partitioner.DIDOIndex;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.JenkinsHash;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DIDOSrv extends AbstractSrv {

    public ConcurrentHashMap<ByteBuffer, DIDOIndex> gigaMaps;

    public DIDOSrv() {
        super();
        this.gigaMaps = new ConcurrentHashMap<>();

        DIDOHandler handler = new DIDOHandler(this);
        this.handler = handler;
        this.processor = new TGraphFSServer.Processor(this.handler);
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src) {
        DIDOIndex gi = surelyGetGigaMap(src);
        Set<Integer> locs = gi.giga_get_all_servers();
        return locs;
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src, int type) {
        return getEdgeLocs(src);
    }

    @Override
    public Set<Integer> getVertexLoc(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = getHashLocation(src, Constants.MAX_VIRTUAL_NODE);
        int physicalIdx = startIdx % this.serverNum;
        locs.add(physicalIdx);
        return locs;
    }

    /**
     * each src vertex is associated with a bitmap which is used as a mask for marking the
     * expanded tree node.
     * @param bsrc
     * @return
     */
    public DIDOIndex surelyGetGigaMap(byte[] bsrc) {
        ByteBuffer src = ByteBuffer.wrap(bsrc);
        int startIdx = getHashLocation(bsrc, Constants.MAX_VIRTUAL_NODE);
        DIDOIndex t = this.gigaMaps.putIfAbsent(src, new DIDOIndex(startIdx, this.serverNum));
        if (t == null) {
            return this.gigaMaps.get(src);
        }
        return t;
    }

    private void initGigaSrvFromDBFile() {
        // Build this.gigaMaps from DB.
        DBKey minDBMeta = DBKey.MinDBKey(Constants.DB_META.getBytes());
        DBKey maxDBMeta = DBKey.MaxDBKey(Constants.DB_META.getBytes());
        //TODO: why do we scan the KV here? why the minDBMeta and maxDBMeta are the same?
        List<KeyValue> r = this.localStore.scanKV(minDBMeta.toKey(), maxDBMeta.toKey());
        for (KeyValue kv : r) {
            byte[] key = kv.getKey();
            DBKey dbKey = new DBKey(key);
            byte[] bsrc = dbKey.dst; //dst is the real key;
            byte[] gigaIndexArray = kv.getValue();
            ByteBuffer src = ByteBuffer.wrap(bsrc);
            DIDOIndex t = new DIDOIndex(gigaIndexArray);
            this.gigaMaps.putIfAbsent(src, t);
        }

        // Build VirtualNodeStatus from DB for each GigaIndex
        DBKey minDBKey = DBKey.MinDBKey();
        DBKey maxDBKey = DBKey.MaxDBKey();
        ArrayList<KeyValue> vals = new ArrayList<KeyValue>();
        byte[] cur = this.localStore.scanLimitedRes(minDBKey.toKey(), maxDBKey.toKey(), Constants.LIMITS, vals);
        while (cur != null) {
            for (KeyValue kv : vals) {
                DBKey dbKey = new DBKey(kv.getKey());
                byte[] src = dbKey.src;
                byte[] dst = dbKey.dst;
                //Let's get ride of src == Constants.DB_META
                if (Arrays.equals(src, Constants.DB_META.getBytes())) {
                    //GLogger.info("[%d] Scan %s:%s", this.localIdx, new String(src), new String(dst));
                    continue;
                }
                JenkinsHash jh = new JenkinsHash();
                int dstHash = Math.abs(jh.hash32(dst));
                DIDOIndex gi = surelyGetGigaMap(src);;
                int vid = gi.giga_get_vid_from_hash(dstHash);
                gi.add_vid_count(vid);
            }
            vals.clear();
            cur = this.localStore.scanLimitedRes(cur, maxDBKey.toKey(), Constants.LIMITS, vals);
        }
    }

    @Override
    public void start() {
        try {
            initGigaSrvFromDBFile();

            /*  ThreadPoolServer
             TServerTransport serverTransport = new TServerSocket(this.port);
             TBinaryProtocol.Factory proFactory = new TBinaryProtocol.Factory();
             TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport).processor(processor).protocolFactory(proFactory);
             //NOTE: TThreadPoolServer could be the best option for concurrent client less than 10,000, check: https://github.com/m1ch1/mapkeeper/wiki/Thrift-Java-Servers-Compared
             args.maxWorkerThreads(this.serverNum * 200);
             TServer server = new TThreadPoolServer(args);
             */
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(this.port);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
            tArgs.processor(processor);
            tArgs.transportFactory(new TFramedTransport.Factory());
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadedSelectorServer(tArgs);

            GLogger.info("[%d] Starting IncrGiga Server at %s:%d", this.getLocalIdx(), this.localAddr, this.port);
            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

}