package edu.ttu.discl.iogp.gserver;

import edu.ttu.discl.iogp.sengine.OrderedRocksDBAPI;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.Constants;
import edu.ttu.discl.iogp.utils.GLogger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractSrv {

    public TGraphFSServer.Iface handler;
    public TGraphFSServer.Processor processor;
    public TGraphFSServer.Client[] clients;
    public ArrayList<TGraphFSServer.Client[]> clientPools;
    public AtomicInteger clientIndex;

    /**
     * Asynchronous Clients
     */
    public TGraphFSServer.AsyncClient[] asyncClients;
    public ArrayList<TGraphFSServer.AsyncClient[]> asyncClientPools;
    public AtomicInteger asyncClientIndex;

    private String dbFile = null;
    private int localIdx = -1;
    private List<String> allSrvs = new ArrayList<>();

    public String localAddr;
    public int port;
    public int serverNum;
    public OrderedRocksDBAPI localstore;
    public ExecutorService workerPool;
    public ExecutorService prefetchPool;

    //Monitoring
    //public final MetricRegistry METRICS = new MetricRegistry();
    public void init() {
        if (dbFile == null || localIdx < 0 || allSrvs.isEmpty()) {
            System.err.println("dbnum, idx or allsrvs are not well-configured. Please check the parameter.");
            System.exit(1);
        }
        String localAddrPort = this.getAllSrvs().get(this.getLocalIdx());
        this.localAddr = localAddrPort.split(":")[0];
        this.port = Integer.parseInt(localAddrPort.split(":")[1]);
        this.serverNum = this.getAllSrvs().size();
        this.localstore = new OrderedRocksDBAPI(this.getDbFile());
        this.clients = new TGraphFSServer.Client[this.serverNum];
        for (int i = 0; i < this.serverNum; i++) {
            this.clients[i] = null;
        }

        this.clientPools = new ArrayList<>(Constants.THRIFT_POOL_SIZE);
        for (int i = 0; i < Constants.THRIFT_POOL_SIZE; i++){
            this.clientPools.add(new TGraphFSServer.Client[this.serverNum]);
        }
        this.clientIndex = new AtomicInteger(0);

        /**
         * Async Clients
         */
        this.asyncClients = new TGraphFSServer.AsyncClient[this.serverNum];
        this.asyncClientPools = new ArrayList<>(Constants.THRIFT_POOL_SIZE);
        for (int i = 0; i < Constants.THRIFT_POOL_SIZE; i++){
            this.asyncClientPools.add(new TGraphFSServer.AsyncClient[this.serverNum]);
        }
        this.asyncClientIndex = new AtomicInteger(0);

    }

    public AbstractSrv() {
        int procs = Runtime.getRuntime().availableProcessors();
        procs = Math.max(procs, 1);
        this.workerPool = Executors.newFixedThreadPool(2 * procs);
        this.prefetchPool = Executors.newFixedThreadPool(4 * procs);
        /*
         ConsoleReporter reporter = ConsoleReporter.forRegistry(METRICS)
         .convertRatesTo(TimeUnit.SECONDS)
         .convertDurationsTo(TimeUnit.MILLISECONDS)
         .build();
         reporter.start(10, TimeUnit.SECONDS);
         */
    }

    public abstract Set<Integer> getEdgeLocs(byte[] src);

    public abstract Set<Integer> getVertexLocation(byte[] src);

    public void runit() {
        init();
        start();
    }

    public void start() {
        try {
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

            GLogger.info("[%d] Starting %s Server at %s:%d", this.getLocalIdx(), this.getClass().getSimpleName(), this.localAddr, this.port);
            server.serve();

        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public synchronized TGraphFSServer.Client getClientConn(int target) throws TTransportException {

        if (clients[target] != null) {
            return clients[target];
        }
        String addrPort = this.getAllSrvs().get(target);
        String taddr = addrPort.split(":")[0];
        int tport = Integer.parseInt(addrPort.split(":")[1]);
        TTransport transport = new TFramedTransport(new TSocket(taddr, tport));
        //TTransport transport = new TSocket(taddr, tport);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TGraphFSServer.Client client = new TGraphFSServer.Client(protocol);
        clients[target] = client;
        return client;
    }



    /**
     * @param target
     * @return AsyncClient
     * @throws TTransportException
     */
    public synchronized TGraphFSServer.AsyncClient getAsyncClientConn(int target) {
        try {
            if (asyncClients[target] != null) {
                return asyncClients[target];
            }
            String addrPort = this.getAllSrvs().get(target);
            String taddr = addrPort.split(":")[0];
            int tport = Integer.parseInt(addrPort.split(":")[1]);

            /*
             Not Sure which one is correct. Try TNonblockingSocket first.
             1. new TNonblockingSocket(taddr, tport) seems not correct
             TTransport transport = new TFramedTransport(new TSocket(taddr, tport));
             transport.open();
             TProtocol protocol = new TBinaryProtocol(transport);
             */
            TGraphFSServer.AsyncClient client
                    = new TGraphFSServer.AsyncClient(new TBinaryProtocol.Factory(),
                            new TAsyncClientManager(),
                            new TNonblockingSocket(taddr, tport));

            asyncClients[target] = client;
            return client;
        } catch (IOException ex) {
            GLogger.error("New AsyncClient Error: %d", target);
            return null;
        } 
    }


    public synchronized TGraphFSServer.Client getClientConnWithPool(int target) throws TTransportException {
        int thisId = (this.clientIndex.getAndIncrement() % Constants.THRIFT_POOL_SIZE);

        if (this.clientPools.get(thisId)[target] != null){
            return this.clientPools.get(thisId)[target];
        }

        String addrPort = this.getAllSrvs().get(target);
        String taddr = addrPort.split(":")[0];
        int tport = Integer.parseInt(addrPort.split(":")[1]);
        TTransport transport = new TFramedTransport(new TSocket(taddr, tport));
        //TTransport transport = new TSocket(taddr, tport);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        TGraphFSServer.Client client = new TGraphFSServer.Client(protocol);
        this.clientPools.get(thisId)[target] = client;
        return client;
    }

    public synchronized TGraphFSServer.AsyncClient getAsyncClientConnWithPool(int target) {
        try {
            int thisId = (this.asyncClientIndex.getAndIncrement() % Constants.THRIFT_POOL_SIZE);

            if (this.asyncClientPools.get(thisId)[target] != null){
                return this.asyncClientPools.get(thisId)[target];
            }

            String addrPort = this.getAllSrvs().get(target);
            String taddr = addrPort.split(":")[0];
            int tport = Integer.parseInt(addrPort.split(":")[1]);

            TGraphFSServer.AsyncClient client
                    = new TGraphFSServer.AsyncClient(new TBinaryProtocol.Factory(),
                    new TAsyncClientManager(),
                    new TNonblockingSocket(taddr, tport));

            this.asyncClientPools.get(thisId)[target] = client;
            return client;
        } catch (IOException ex) {
            GLogger.error("New AsyncClient Error: %d", target);
            return null;
        }
    }

    public synchronized void closeClientConn(int target) {
        clients[target].getOutputProtocol().getTransport().close();
    }

    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }

    public int getLocalIdx() {
        return localIdx;
    }

    public void setLocalIdx(int localIdx) {
        this.localIdx = localIdx;
    }

    public List<String> getAllSrvs() {
        return allSrvs;
    }

    public void setAllSrvs(List<String> allSrvs) {
        this.allSrvs = allSrvs;
    }
}
