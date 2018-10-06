package edu.dair.sgdb.gserver.giga;

import edu.dair.sgdb.partitioner.GigaIndex;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.JenkinsHash;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GigaSplitWorker implements Runnable {

    public class OneSplit {

        private ByteBuffer src;
        private GigaIndex gi;
        private int index, currVid, newIndex, server, newVid;

        public OneSplit(ByteBuffer src, GigaIndex gi, int index,
                        int vid, int new_index, int new_vid, int new_server) {
            this.gi = gi;
            this.src = src;
            this.index = index;
            this.currVid = vid;
            this.newIndex = new_index;
            this.server = new_server;
            this.newVid = new_vid;
        }
    }
    private GIGASrv instance;
    private LinkedBlockingQueue<OneSplit> splits;
    //private final Counter pendingSplits = GIGASrv.METRICS.counter(MetricRegistry.name(GigaSplitWorker.class, "SplitJob", "QueueSize"));

    public GigaSplitWorker(GIGASrv s) {
        this.instance = s;
        this.splits = new LinkedBlockingQueue<OneSplit>();
    }

    public void addSplit(ByteBuffer src, GigaIndex gi, int index, int new_index) {
        int vid = gi.giga_get_vid_from_index(index);
        int new_vid = gi.giga_get_vid_from_index(new_index);
        int new_server = gi.giga_get_server_from_index(new_index);

        OneSplit s = new OneSplit(src, gi, index, vid, new_index, new_vid, new_server);
        try {
            this.splits.put(s);
        } catch (InterruptedException ex) {
            Logger.getLogger(GigaSplitWorker.class.getName()).log(Level.SEVERE, null, ex);
        }

        //pendingSplits.inc();
        /*
         GIGASrv.METRICS.register(MetricRegistry.name(GigaSplitWorker.class, "SplitJob", "QueueSize"),
         new Gauge<Integer>(){
         @Override
         public Integer getValue(){
         return splits.size();
         }
         });
         */
    }

    private void persistentGigaIndex(byte[] src, GigaIndex gi) {
        DBKey dbMetaKey = new DBKey(Constants.DB_META.getBytes(), src, 0);
        instance.localStore.put(dbMetaKey.toKey(), gi.toByteArray());
    }

    @Override
    public void run() {
        while (true) {
            try {
                OneSplit split = this.splits.take();
                //this.pendingSplits.dec();
                ByteBuffer src = split.src;
                byte[] bsrc = NIOHelper.getActiveArray(src);
                int index = split.index;
                int currVid = split.currVid;
                int newIndex = split.newIndex;
                int server = split.server;
                int newVid = split.newVid;
                GigaIndex gi = split.gi;

                /*
                 System.out.println("Split index[" + index
                 + "] from vid[" + currVid + "] stored in server[" + instance.getLocalIdx()
                 + "] to index[" + newIndex + "] from vid["
                 + newVid + "] stored in server[" + server + "]");
                 System.out.println("Before Splitting, Bitmap: ");
                 GigaIndex.pprint(gi.bitmap);
                 */
                ArrayList<KeyValue> vals = new ArrayList<KeyValue>();
                ArrayList<byte[]> removedKeys = new ArrayList<>();

                GigaIndex.VirtualNodeStatus currVns = gi.surelyGetVirtNodeStatus(currVid);
                GigaIndex.VirtualNodeStatus nvns = gi.surelyGetVirtNodeStatus(newVid);
                JenkinsHash jh = new JenkinsHash();
                DBKey startKey = DBKey.MinDBKey(bsrc);
                DBKey endKey = DBKey.MaxDBKey(bsrc);

                synchronized (gi) {

                    if (server != instance.getLocalIdx()) {
                        byte[] cur = instance.localStore.scanLimitedRes(startKey.toKey(), endKey.toKey(), Constants.LIMITS, vals);
                        while (cur != null) {
                            ArrayList<KeyValue> movs = new ArrayList<KeyValue>();
                            for (KeyValue kv : vals) {
                                DBKey dbKey = new DBKey(kv.getKey());
                                byte[] dstKey = dbKey.dst;
                                int distHash = Math.abs(jh.hash32(dstKey));
                                //GigaIndex.pprint(instance.localIdx, gi.bitmap);
                                if (gi.giga_should_split(distHash, newIndex)) {
                                    //GLogger.info("[%d] %s needs move to other server", instance.localIdx, new String(dstKey));
                                    movs.add(kv);
                                }
                            }

                            if (movs.size() > 0) {
                                try {
                                    TGraphFSServer.Client target = instance.getClientConn(server);
                                    target.giga_batch_insert(src, newVid, movs);
                                    instance.releaseClientConn(server, target);
                                } catch (TException e) {
                                    e.printStackTrace();
                                }

                                for (KeyValue p : movs) {
                                    removedKeys.add(p.getKey());
                                }
                            }

                            vals.clear();
                            movs.clear();
                            cur = instance.localStore.scanLimitedRes(cur, endKey.toKey(), Constants.LIMITS, vals);
                        }
                        currVns.decr_size(removedKeys.size());
                    }

                    /**
                     * After sending all data to remote location, we update
                     * local bitmap any operation later will go to new server.
                     */
                    //MARK: this has to be newIndex - 1
                    gi.giga_split_update_mapping(index, newIndex - 1);
                    persistentGigaIndex(bsrc, gi);
                    currVns.finishSplit();

                    /**
                     * Tell target server to update its own bitmap if target
                     * server is not local server.
                     */
                    if (server != instance.getLocalIdx()) {
                        try {
                            TGraphFSServer.Client target = instance.getClientConn(server);
                            //synchronized (target) {
                            target.giga_split(src, newVid, Constants.SPLIT_END, ByteBuffer.wrap(gi.bitmap));
                            //}
                            instance.releaseClientConn(server,target);
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }

                }

                // delete local copies.
                for (byte[] k : removedKeys) {
                    instance.localStore.remove(k);
                    //GLogger.info("[%d] Removes: %s", instance.localIdx, new String(new DBKey(k).dst));
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(GigaSplitWorker.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

}
