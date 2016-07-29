package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.gserver.AbstractSrv;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.GLogger;
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
        locs.add(getEdgeLoc(src, this.serverNum));

        /*
         * if split(v) == 1 and we are asking EdgeType.OUT
         */
        if (split.containsKey(bbsrc) && split.get(bbsrc) == 1 && type == EdgeType.OUT.get())
            for (int i = 0; i < this.serverNum; i++)
                locs.add(i);

        /*
         * If split(v) == 2 and we are asking EdgeType.IN
         */
        if (split.containsKey(bbsrc) && split.get(bbsrc) == 2 && type == EdgeType.IN.get())
            for (int i = 0; i < this.serverNum; i++)
                locs.add(i);

        return locs;
    }

	/**
     * @param src
     * @return A set of interger, indicating vertex location. All current algorithms will return
     * a set with only one element
     *
     * @TODO: Not working for unlocal vertices yet.
     * IOGP makes this function tricky. During traversal, after visiting an edge, we need
     * to find locations for all destination vertices. This might include visiting the original
     * server asking for current location.
     *
     * Caching real location in this node needes retries mechanism in traversal engine.
     */
    @Override
    public Set<Integer> getVertexLoc(byte[] src) {
        ByteBuffer bbsrc = ByteBuffer.wrap(src);
        Set<Integer> locs = new HashSet<>();

        int hashIdx = getEdgeLoc(src, this.serverNum);

        if (loc.containsKey(bbsrc)) {
            locs.add(loc.get(bbsrc));
            return locs;
        }

        return locs;
    }

}
