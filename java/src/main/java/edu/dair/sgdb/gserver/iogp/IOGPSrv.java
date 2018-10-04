package edu.dair.sgdb.gserver.iogp;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.GLogger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IOGPSrv extends AbstractSrv {

    /*
     * split(v) = 0//No Split, 1//Split OutEdge, 2//Split InEdge
     */
    HashMap<ByteBuffer, Integer> split;
    HashMap<ByteBuffer, Integer> loc;
    ConcurrentHashMap<ByteBuffer, Counters> edgecounters;
    IOGPGraphSplitMover spliter = null;
    IOGPGraphReassigner reassigner = null;

    AtomicInteger size = new AtomicInteger(0);

    /**
     * These two information must be always correct!
     */
    HashMap<ByteBuffer, Integer> syncedLocationInfo = new HashMap<>();
    HashMap<ByteBuffer, Integer> syncedSplitInfo = new HashMap<>();

    private int getLocationFromSyncedInfo(byte[] src) {
        if (syncedLocationInfo.containsKey(ByteBuffer.wrap(src)))
            return syncedLocationInfo.get(ByteBuffer.wrap(src));
        else
            return getHashLocation(src, this.serverNum);
    }

    private int getSplitInfoFromSyncedInfo(byte[] src) {
        if (syncedSplitInfo.containsKey(ByteBuffer.wrap(src)))
            return syncedSplitInfo.get(ByteBuffer.wrap(src));
        else
            return 0;
    }

    public IOGPSrv() {
        super();
        this.handler = new IOGPHandler(this);
        this.processor = new TGraphFSServer.Processor(this.handler);

        split = new HashMap<>();
        loc = new HashMap<>();
        edgecounters = new ConcurrentHashMap<>();

    }


    public void start() {
        /**
         * IOGP Workers. Have to be after init();
         */
        spliter = new IOGPGraphSplitMover(this);
        spliter.startWorkers();
        reassigner = new IOGPGraphReassigner(this);

        try {
            //About Thrift server: http://www.voidcn.com/article/p-xpdesbbf-ks.html
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(this.port);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
            tArgs.processor(processor);
            tArgs.transportFactory(new TFramedTransport.Factory(1024 * 1024 * 1024));
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadedSelectorServer(tArgs);

            GLogger.info("[%d] Starting IOGP Server at %s:%d",
                    this.getLocalIdx(), this.localAddr, this.port);

            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src, int type) {

        ByteBuffer bbsrc = ByteBuffer.wrap(src);
        Set<Integer> locs = new HashSet<>();

        //by default, split(v) == 0 and src location is where edges are stored
        locs.add(getLocationFromSyncedInfo(src));

        /*
         * if split(v) == 1 and we are asking EdgeType.OUT
         */
        if (getSplitInfoFromSyncedInfo(src) == 1)
            for (int i = 0; i < this.serverNum; i++)
                locs.add(i);

        return locs;
    }


    @Override
    public Set<Integer> getEdgeLocs(byte[] src) {
        return getEdgeLocs(src, 0);
    }


    @Override
    public Set<Integer> getVertexLoc(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        locs.add(getLocationFromSyncedInfo(src));
        return locs;
    }

}
