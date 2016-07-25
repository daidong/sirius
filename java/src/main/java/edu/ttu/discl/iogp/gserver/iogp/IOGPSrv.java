package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.gserver.AbstractSrv;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.JenkinsHash;
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

public class IOGPSrv extends AbstractSrv {

    /*
     * split(v) = 0//No Split, 1//Split OutEdge, 2//Split InEdge
     */
    HashMap<ByteBuffer, Integer> split;
    HashMap<ByteBuffer, Integer> loc;
    HashMap<ByteBuffer, Counters> ec;

    int size = 0;

    public class Counters {
        int alo = 0, ali = 0, plo = 0, pli = 0;
    }

    public IOGPSrv() {
        super();
        this.handler = new IOGPHandler(this);
        this.processor = new TGraphFSServer.Processor(this.handler);

        split = new HashMap<>();
        loc = new HashMap<>();
        ec = new HashMap<>();
    }

    public void start() {
        try {
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(this.port);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
            tArgs.processor(processor);
            tArgs.transportFactory(new TFramedTransport.Factory());
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadedSelectorServer(tArgs);
            
            GLogger.info("[%d] Starting IOGP Server at %s:%d", this.getLocalIdx(), this.localAddr, this.port);
            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src) {
        ByteBuffer bbsrc = ByteBuffer.wrap(src);
        Set<Integer> locs = new HashSet<>();

        if (!split.containsKey(bbsrc)
                || (split.containsKey(bbsrc) && split.get(bbsrc) == 0)) {

        } else if (split.containsKey(bbsrc) && split.get(bbsrc) == 1) {

        } else if (split.containsKey(bbsrc) && split.get(bbsrc) == 2) {

        }

        return locs;
    }

    @Override
    public Set<Integer> getVertexLocation(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = getEdgeLocation(src, this.serverNum);
        locs.add(startIdx);
        return locs;
    }

    private int getEdgeLocation(byte[] src, int serverNum) {
        JenkinsHash jh = new JenkinsHash();
        int hashi = Math.abs(jh.hash32(src));
        return (hashi % serverNum);
    }
}
