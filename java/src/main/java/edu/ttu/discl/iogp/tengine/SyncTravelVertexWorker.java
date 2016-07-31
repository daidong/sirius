package edu.ttu.discl.iogp.tengine;

import edu.ttu.discl.iogp.gserver.AbstractSrv;
import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.tengine.prefetch.TravelLocalReaderWithCache;
import edu.ttu.discl.iogp.tengine.travel.SingleRestriction;
import edu.ttu.discl.iogp.tengine.travel.SingleStep;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.thrift.TravelCommand;
import edu.ttu.discl.iogp.thrift.TravelCommandType;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.*;

public class SyncTravelVertexWorker implements Runnable {

    AbstractSrv instance;
    SyncTravelEngine engine;
    long travelId = 0;
    int stepId = 0;
    int replyTo = 0;
    int getFrom = 0;
    long ts = 0L;
    Set<Integer> targets = null;

    public SyncTravelVertexWorker(AbstractSrv s, SyncTravelEngine e, long tid, int sid,
            int replyTo, int getFrom,
            long ts) {
        this.instance = s;
        this.engine = e;
        this.travelId = tid;
        this.stepId = sid;
        this.replyTo = replyTo;
        this.getFrom = getFrom;
        this.ts = ts;
    }

    @Override
    public void run() {
        boolean lastStep = false;
        ArrayList<SingleStep> travelPlan = engine.getSyncTravelPlan(travelId);
        SingleStep currStep = travelPlan.get(stepId);
        if (stepId == (travelPlan.size() - 1)) {
            lastStep = true;
        }

        int tid = engine.mid.addAndGet(1);
        GLogger.debug("S VR %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());
        /*
        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(this.inst.localstore,
                engine.getSyncTravelVertices(this.travelId, this.stepId), currStep, ts);
        */
        ArrayList<byte[]> passedVertices = TravelLocalReaderWithCache.filterVertices(this.instance.localstore, engine.getSyncTravelVertices(this.travelId, this.stepId), currStep, ts);

        GLogger.debug("R VR %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());

        if (lastStep) {
            TravelCommand tc = new TravelCommand();
            tc.setType(TravelCommandType.SYNC_TRAVEL_RTN).setTravelId(travelId)
                    .setStepId(stepId).setGet_from(getFrom).setLocal_id(instance.getLocalIdx());

            ArrayList<KeyValue> vals = new ArrayList<>();
            for (byte[] vertex : passedVertices) {
                KeyValue kv = new KeyValue();
                kv.setKey(vertex);
                vals.add(kv);
            }
            tc.setVals(vals);
            try {
                TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
                GLogger.debug("S TR %d %d %d", instance.getLocalIdx(), replyTo, System.nanoTime());
                client.syncTravelRtn(tc);
                /*
                Client client = inst.getClientConn(replyTo);
                synchronized (client) {
                    GLogger.info("S TR %d %d %d", inst.getLocalIdx(), replyTo, System.nanoTime());
                    client.syncTravelRtn(tc);
                }
                */
            } catch (TException e) {
                e.printStackTrace();
            }

        } else {

            // Already verify the vertices; We need to verify the edges before generating next-step vertices
            HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
            for (byte[] key : passedVertices) {
                Set<Integer> servers = instance.getEdgeLocs(key, EdgeType.OUT.get());
                for (Integer s : servers) {
                    if (!perServerVertices.containsKey(s)) {
                        perServerVertices.put(s, new HashSet<ByteBuffer>());
                    }
                    perServerVertices.get(s).add(ByteBuffer.wrap(key));
                }
            }

            // report to coordinator, round 'stepId' should add these
            // many extra servers besides vertices locations
            List<Integer> addrs = new ArrayList<Integer>(perServerVertices.keySet());

            TravelCommand tc_ext = new TravelCommand();
            tc_ext.setTravelId(travelId)
                    .setStepId(stepId).setType(TravelCommandType.SYNC_TRAVEL_EXTEND)
                    .setGet_from(instance.getLocalIdx()).setExt_srv(addrs)
                    .setLocal_id(instance.getLocalIdx())
                    .setSub_type(1);  //subtype==1 means we are extending to access edges
            //GLogger.warn("[%d] SyncTravelVertexWorker stepId[%d] Extend %d to %s (%d)",
            //        inst.getLocalIdx(), stepId,
            //        inst.getLocalIdx(), addrs, 1);

            try {
                TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
                GLogger.debug("S TE %d %d %d", instance.getLocalIdx(), replyTo, System.nanoTime());
                client.syncTravelExtend(tc_ext);
                /*
                Client client = inst.getClientConn(replyTo);
                synchronized (client) {
                    GLogger.info("S TE %d %d %d", inst.getLocalIdx(), replyTo, System.nanoTime());
                    client.syncTravelExtend(tc_ext);
                }
                */
            } catch (TException e) {
                e.printStackTrace();
            }

            //send travel commands to all servers with the edges
            this.targets = new HashSet<>(perServerVertices.keySet());
            for (int s : perServerVertices.keySet()) {
                List<byte[]> nextKeys = new ArrayList<>();
                for (ByteBuffer bb : perServerVertices.get(s)) {
                    byte[] tbb = NIOHelper.getActiveArray(bb);
                    nextKeys.add(tbb);
                }

                travelPlan.get(stepId).vertexKeyRestrict =
                        new SingleRestriction.InWithValues("key".getBytes(), nextKeys);
                String travelPayLoad = engine.serializeTravelPlan(travelPlan);

                TravelCommand tc1 = new TravelCommand();
                tc1.setType(TravelCommandType.SYNC_TRAVEL).setTravelId(travelId)
                        .setStepId(stepId).setReply_to(replyTo).setTs(ts).setPayload(travelPayLoad)
                        .setGet_from(instance.getLocalIdx()).setLocal_id(s)
                        .setSub_type(1);

                try {
                    GLogger.debug("S TV %d %d %d %d", instance.getLocalIdx(), s, System.nanoTime(), 1);
                    GLogger.debug("[%d] SyncTravelVertexWorker send SyncTravel to %d",
                            instance.getLocalIdx(), s);

                    TGraphFSServer.AsyncClient aclient = instance.getAsyncClientConnWithPool(s);
                    aclient.syncTravel(tc1, new BroadCastTVCallback(s));
                    /*
                    TGraphFSServer.AsyncClient aclient = inst.getAsyncClientConn(s);
                    synchronized (aclient) {
                        aclient.syncTravel(tc1, new BroadCastTVCallback(s));
                    }
                    */
                } catch (TException te) {
                    GLogger.error("SyncTravelVertexWorker Error");
                }
            }
        }
        //System.out.println("One Round of SyncStart costs: " + (System.currentTimeMillis() - syncStartAt));
    }

    class BroadCastTVCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.syncTravel_call> {

        int finished;

        public BroadCastTVCallback(int f) {
            this.finished = f;
        }

        @Override
        public void onComplete(TGraphFSServer.AsyncClient.syncTravel_call t) {
            synchronized (targets) {
                targets.remove(this.finished);

                GLogger.debug("[%d] VertexBroadcastTVCallback Complete sending %d, %s not finish yet",
                        instance.getLocalIdx(), this.finished, targets);

                if (targets.isEmpty()) { //finish all sends, send SYNC_TRAVEL_FINISH Command

                    TravelCommand tc3 = new TravelCommand();
                    tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH)
                            .setTravelId(travelId)
                            .setStepId(stepId)
                            .setGet_from(getFrom)
                            .setLocal_id(instance.getLocalIdx())
                            .setReply_to(replyTo)
                            .setTs(ts)
                            .setSub_type(0); //finish on edges

                    try {
                        TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
                        GLogger.debug("S TF %d %d %d",
                                instance.getLocalIdx(),
                                replyTo, System.nanoTime());
                        client.syncTravelFinish(tc3);

                    } catch (TException e) {
                        e.printStackTrace();
                    }
                    return;
                }
            }
        }

        @Override
        public void onError(Exception excptn) {
            GLogger.error("SyncTravelVertexWorker BroadCastTVCallback Error: %s", excptn.getMessage());
        }

    }
}
