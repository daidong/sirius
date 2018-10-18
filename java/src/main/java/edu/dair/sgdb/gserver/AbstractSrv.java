package edu.dair.sgdb.gserver;

import edu.dair.sgdb.sengine.OrderedRocksDBAPI;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.JenkinsHash;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public abstract class AbstractSrv {

    public TGraphFSServer.Iface handler;
    public TGraphFSServer.Processor processor;

    /**
     * Synchronous Clients
     */
    public HashMap<Integer, ConcurrentLinkedQueue<TGraphFSServer.Client>> clientPools;

    private String dbFile = null;
    private int localIdx = -1;
    private List<String> allSrvs = new ArrayList<>();

    public String localAddr;
    public int port;
    public int serverNum;
    public OrderedRocksDBAPI localStore;
    public ExecutorService workerPool;
    public ExecutorService prefetchPool;

    //Monitoring
    //public final MetricRegistry METRICS = new MetricRegistry();
    public void init() {
        if (dbFile == null || localIdx < 0 || allSrvs.isEmpty()) {
            System.err.println("dbnum, idx or allsrvs are not well-configured. " +
                    "Please check the parameter.");
            System.exit(1);
        }
        String localAddrPort = this.getAllSrvs().get(this.getLocalIdx());
        this.localAddr = localAddrPort.split(":")[0];
        this.port = Integer.parseInt(localAddrPort.split(":")[1]);
        this.serverNum = this.getAllSrvs().size();
        this.localStore = new OrderedRocksDBAPI(this.getDbFile());

        /**
         * Sync Clients
         */
        this.clientPools = new HashMap<>(this.serverNum);
        for (int i = 0; i < this.serverNum; i++)
            this.clientPools.put(i, new ConcurrentLinkedQueue<TGraphFSServer.Client>());

    }

    public AbstractSrv() {
        int procs = Runtime.getRuntime().availableProcessors();
        procs = Math.max(procs, 1);
        this.workerPool = Executors.newFixedThreadPool(Constants.WORKER_THREAD_FACTOR * procs);
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

    public abstract Set<Integer> getEdgeLocs(byte[] src, int type);

    public abstract Set<Integer> getVertexLoc(byte[] src);


    public void runit() {
        init();
        start();
    }

    abstract public void start();


    public TGraphFSServer.Client getClientConn(int target) throws TTransportException {

        synchronized (this.clientPools) {
            if (!this.clientPools.containsKey(target))
                this.clientPools.put(target, new ConcurrentLinkedQueue<TGraphFSServer.Client>());
        }

        ConcurrentLinkedQueue<TGraphFSServer.Client> pool = this.clientPools.get(target);

        synchronized (pool) {
            if (pool.isEmpty()) {
                String addrPort = this.getAllSrvs().get(target);
                String taddr = addrPort.split(":")[0];
                int tport = Integer.parseInt(addrPort.split(":")[1]);
                TTransport transport = new TFramedTransport(new TSocket(taddr, tport), 2 * 1024 * 1024);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                TGraphFSServer.Client client = new TGraphFSServer.Client(protocol);
                return client;
            } else {
                TGraphFSServer.Client client = pool.poll(); //retrieve and remove the first element
                return client;
            }
        }
    }

    public boolean releaseClientConn(int target, TGraphFSServer.Client c){
        ConcurrentLinkedQueue<TGraphFSServer.Client> pool = this.clientPools.get(target);
        synchronized (pool) {
            pool.add(c);
        }
        return true;
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

    public int getHashLocation(byte[] src, int serverNum) {
        JenkinsHash jh = new JenkinsHash();
        int hashi = Math.abs(jh.hash32(src));
        return (hashi % serverNum);
    }
}
