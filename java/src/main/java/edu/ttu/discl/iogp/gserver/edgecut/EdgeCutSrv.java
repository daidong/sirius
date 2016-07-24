package edu.ttu.discl.iogp.gserver.edgecut;

import edu.ttu.discl.iogp.gserver.AbstractSrv;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.HashKeyLocation;
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
        EdgeCutHandler handler = new EdgeCutHandler(this);
        this.handler = handler;
        this.processor = new TGraphFSServer.Processor(this.handler);
    }

    public void start() {
        try {
            /*
            TServerTransport serverTransport = new TServerSocket(this.port);
            Factory proFactory = new Factory();
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
            
            GLogger.info("[%d] Starting EdgeCut Server at %s:%d", this.getLocalIdx(), this.localAddr, this.port);
            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<Integer> getEdgeLocs(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = HashKeyLocation.getEdgeLocation(src, this.serverNum);
        locs.add(startIdx);
        return locs;
    }

    @Override
    public Set<Integer> getVertexLocation(byte[] src) {
        Set<Integer> locs = new HashSet<>();
        int startIdx = HashKeyLocation.getEdgeLocation(src, this.serverNum);
        locs.add(startIdx);
        return locs;
    }

}
