package edu.dair.sgdb.gserver.edgecut;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.GLogger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import java.util.HashSet;
import java.util.Set;

public class EdgeCutSrv extends AbstractSrv {

    public EdgeCutSrv() {
        super();
        this.handler = new EdgeCutHandler(this);
        this.processor = new TGraphFSServer.Processor(this.handler);
    }


    public void start() {
        try {
            //About Thrift server: http://www.voidcn.com/article/p-xpdesbbf-ks.html
            TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(this.port);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
            tArgs.processor(processor);
            tArgs.transportFactory(new TFramedTransport.Factory(1024 * 1024 * 1024));
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TThreadedSelectorServer(tArgs);

            GLogger.info("[%d] Starting EdgeCut Server at %s:%d", this.getLocalIdx(), this.localAddr, this.port);
            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src) {
        return getEdgeLocs(src, 0);
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src, int type) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = getHashLocation(src, this.serverNum);
        locs.add(startIdx);
        return locs;
    }

    @Override
    public Set<Integer> getVertexLoc(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = getHashLocation(src, this.serverNum);
        locs.add(startIdx);
        return locs;
    }

}
