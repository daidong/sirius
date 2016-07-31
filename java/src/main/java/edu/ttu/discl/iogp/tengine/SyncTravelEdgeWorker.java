package edu.ttu.discl.iogp.tengine;

import edu.ttu.discl.iogp.gserver.AbstractSrv;
import edu.ttu.discl.iogp.tengine.prefetch.TravelLocalReaderWithCache;
import edu.ttu.discl.iogp.tengine.travel.SingleRestriction;
import edu.ttu.discl.iogp.tengine.travel.SingleStep;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.thrift.TravelCommand;
import edu.ttu.discl.iogp.thrift.TravelCommandType;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.*;

public class SyncTravelEdgeWorker implements Runnable {

    AbstractSrv instance;
    SyncTravelEngine engine;
    long travelId = 0;
    int stepId = 0;
    int getFrom = 0;
    int replyTo = 0;
    long ts = 0;
    HashSet<ByteBuffer> keys;
    Set<Integer> targets = null;

    public SyncTravelEdgeWorker(AbstractSrv s,
                                SyncTravelEngine e,
                                long tid,
                                int sid,
                                int getFrom,
                                int replyTo,
                                long ts,
                                HashSet<ByteBuffer> kSets) {
        this.instance = s;
        this.engine = e;
        this.travelId = tid;
        this.stepId = sid;
        this.getFrom = getFrom;
        this.replyTo = replyTo;
        this.ts = ts;
        this.keys = kSets;
    }

    @Override
    public void run() {
        GLogger.info("Server [%d] Start Read Edges for stepId [%d], Command from %d",
                instance.getLocalIdx(), stepId, getFrom);

        ArrayList<SingleStep> travelPlan = engine.getSyncTravelPlan(travelId);
        SingleStep currStep = travelPlan.get(stepId);

        ArrayList<byte[]> passedVertices = new ArrayList<>();
        for (ByteBuffer k : keys) {
            byte[] tk = NIOHelper.getActiveArray(k);
            passedVertices.add(tk);
        }
        HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();

        int tid = engine.mid.addAndGet(1);
        GLogger.info("Server [%d] Read Local Edges for %d at %d",
                instance.getLocalIdx(), stepId, System.nanoTime());
        /*
        HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(this.inst.localstore,
                passedVertices, currStep, ts);
        */

        HashSet<byte[]> nextVertices = TravelLocalReaderWithCache.scanLocalEdges(
                this.instance.localstore,
                engine.pool, passedVertices, currStep, ts);

        GLogger.info("Server [%d] Finish Read Local Edges for %d at %d",
                instance.getLocalIdx(), stepId, System.nanoTime());

        for (byte[] v : nextVertices) {
            Set<Integer> servers = instance.getVertexLoc(v);
            for (Integer s : servers) {
                if (!perServerVertices.containsKey(s)) {
                    perServerVertices.put(s, new HashSet<ByteBuffer>());
                }
                perServerVertices.get(s).add(ByteBuffer.wrap(v));
            }
        }


        /**
         * Create an asynchronous thread (preemptable) to pre-fetch +1 step vertices and edges
         
        if ((stepId)< travelPlan.size()) {
            SyncTravelPreFetcher fetcher = new SyncTravelPreFetcher(inst, engine,
                    perServerVertices.get(inst.getLocalIdx()), travelPlan,
                    travelId, stepId, ts);
            fetcher.start();
        }
        */

        // report to coordinator, round 'stepId + 1' should have these many servers, and they are:
        List<Integer> addrs = new ArrayList<Integer>(perServerVertices.keySet());
        TravelCommand tc1 = new TravelCommand();
        tc1.setTravelId(travelId).setStepId(stepId + 1).setType(TravelCommandType.SYNC_TRAVEL_EXTEND)
                .setGet_from(instance.getLocalIdx()).setExt_srv(addrs)
                .setLocal_id(instance.getLocalIdx())
                .setSub_type(0); //subtype==0 means we are extending vertices;
        //GLogger.warn("[%d] SyncTravelEdgeWorker stepId[%d] GetFrom: %d, Extend %d to %s (%d)",
        //        inst.getLocalIdx(), (stepId + 1), getFrom, inst.getLocalIdx(), addrs, 0);

        try {
            TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
            GLogger.debug("%d Send TravelExtend to %d at %d",
                    instance.getLocalIdx(), replyTo, System.nanoTime());

            client.syncTravelExtend(tc1);

            /*
            Client client = inst.getClientConn(replyTo);
            synchronized (client) {
                GLogger.info("S TE %d %d %d", inst.getLocalIdx(), replyTo, System.nanoTime());
                client.syncTravelExtend(tc1);
            }
            */
        } catch (TException e) {
            e.printStackTrace();
        }

        // send sync_travel command to all covered servers
        this.targets = new HashSet<>(perServerVertices.keySet());
        GLogger.debug("EdgeWorker Broadcast to %s", this.targets);

        for (int s : perServerVertices.keySet()) {
            GLogger.debug("EdgeWorker Broadcast to server %d", s);

            List<byte[]> nextKeys = new ArrayList<byte[]>();
            for (ByteBuffer bb : perServerVertices.get(s)) {
                byte[] tbb = NIOHelper.getActiveArray(bb);
                nextKeys.add(tbb);
            }

            travelPlan.get(stepId + 1).vertexKeyRestrict = new SingleRestriction.InWithValues("key".getBytes(), nextKeys);
            String travelPayLoad = engine.serializeTravelPlan(travelPlan);

            TravelCommand tc2 = new TravelCommand();
            tc2.setType(TravelCommandType.SYNC_TRAVEL)
                    .setTravelId(travelId).setStepId(stepId + 1)
                    .setReply_to(replyTo).setGet_from(instance.getLocalIdx())
                    .setPayload(travelPayLoad).setTs(ts)
                    .setSub_type(0);

            try {
                GLogger.info("%d Send Travel Comand to %d at %d for vertex",
                        instance.getLocalIdx(), s, System.nanoTime());

                if (s == instance.getLocalIdx()) {
                    //next vertices are stored locally. Directly process them.
                    engine.incrEdge2DstLocalCounter(travelId, nextKeys.size());
                    //engine.syncTravel(tc2);
                }

                TGraphFSServer.AsyncClient aclient = instance.getAsyncClientConnWithPool(s);
                aclient.syncTravel(tc2, new EdgeBroadCastTVCallback(s));

                /*
                AsyncClient aclient = inst.getAsyncClientConn(s);
                synchronized (aclient) {
                    aclient.syncTravel(tc2, new EdgeBroadCastTVCallback(s));
                }
                */
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        if (perServerVertices.keySet().isEmpty()){
            //it may be empty! Probably some bugs in GigaIndex
            TravelCommand tc3 = new TravelCommand();
            tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH).setTravelId(travelId)
                    .setStepId(stepId).setGet_from(getFrom).setLocal_id(instance.getLocalIdx())
                    .setReply_to(replyTo).setTs(ts).setSub_type(1); //finish on edges

            //GLogger.warn("[%d] SyncTravelEdgeWorker stepId[%d] Finish %d to %s (%d)",
            //        inst.getLocalIdx(), (stepId), getFrom, inst.getLocalIdx(), 1);

            try {
                TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
                GLogger.info("%d Send TravelFinish to %d at %d on reading edges",
                        instance.getLocalIdx(), replyTo, System.nanoTime());

                client.syncTravelFinish(tc3);

            } catch (TException e) {
                e.printStackTrace();
            }
        }

    }

    class EdgeBroadCastTVCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.syncTravel_call> {

        int finished;

        public EdgeBroadCastTVCallback(int f) {
            this.finished = f;
        }

        @Override
        public void onComplete(TGraphFSServer.AsyncClient.syncTravel_call t) {
            synchronized (targets) {
                targets.remove(this.finished);

                GLogger.debug("[%d] EdgeCallback Finish %d to %d(%d), Complete sending %d, %s not finish yet", instance.getLocalIdx(), getFrom, instance.getLocalIdx(), 1, this.finished, targets);

                if (targets.isEmpty()) { //finish all sends, send SYNC_TRAVEL_FINISH Command

                    TravelCommand tc3 = new TravelCommand();
                    tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH).setTravelId(travelId)
                            .setStepId(stepId).setGet_from(getFrom).setLocal_id(instance.getLocalIdx())
                            .setReply_to(replyTo).setTs(ts).setSub_type(1); //finish on edges

                    //GLogger.warn("[%d] SyncTravelEdgeWorker stepId[%d] Finish %d to %s (%d)",
                    //        inst.getLocalIdx(), (stepId), getFrom, inst.getLocalIdx(), 1);

                    try {
                        TGraphFSServer.Client client = instance.getClientConnWithPool(replyTo);
                        GLogger.info("%d Send TravelFinish to %d at %d on reading edges",
                                instance.getLocalIdx(), replyTo, System.nanoTime());

                        client.syncTravelFinish(tc3);

                        /*
                        Client client = inst.getClientConn(replyTo);
                        synchronized (client) {
                            GLogger.info("S TF %d %d %d", inst.getLocalIdx(), replyTo, System.nanoTime());
                            client.syncTravelFinish(tc3);
                        }
                        */
                    } catch (TException e) {
                        e.printStackTrace();
                    }

                    return;
                }
            }
        }

        @Override
        public void onError(Exception excptn) {
            GLogger.error("Error [%d] Send TV to [%d]", instance.getLocalIdx(), this.finished);
            GLogger.error("SyncTravelEdgeWorker BroadCastTVCallback Error: %s", excptn);
        }

    }
}
