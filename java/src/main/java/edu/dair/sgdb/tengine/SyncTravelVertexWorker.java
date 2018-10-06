package edu.dair.sgdb.tengine;

import edu.dair.sgdb.tengine.travel.SingleRestriction;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.thrift.TGraphFSServer;
import edu.dair.sgdb.thrift.TravelCommand;
import edu.dair.sgdb.thrift.TravelCommandType;
import edu.dair.sgdb.utils.GLogger;
import edu.dair.sgdb.utils.NIOHelper;
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
        GLogger.info("Server %d Read Local Vertices for %d at %d",
                instance.getLocalIdx(), stepId, System.nanoTime());
        /*
        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(this.inst.localStore,
                engine.getSyncTravelVertices(this.travelId, this.stepId), currStep, ts);
        */
        ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(
                this.instance.localStore,
                engine.getSyncTravelVertices(this.travelId, this.stepId),
                currStep, ts);

        GLogger.info("Server %d Finish Read %d Local Vertices for %d at %d",
                instance.getLocalIdx(), passedVertices.size(), stepId, System.nanoTime());

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
                TGraphFSServer.Client client = instance.getClientConn(replyTo);
                GLogger.info("%d Send TravelRtn to %d at %d",
                        instance.getLocalIdx(), replyTo, System.nanoTime());

                client.syncTravelRtn(tc);
                instance.releaseClientConn(replyTo, client);
            } catch (TException e) {
                e.printStackTrace();
            }

        } else {

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
                    .setSub_type(1);

            try {
                TGraphFSServer.Client client = instance.getClientConn(replyTo);
                GLogger.info("%d Send TraveExtend to %d including %s at %d",
                        instance.getLocalIdx(), replyTo, addrs, System.nanoTime());
                client.syncTravelExtend(tc_ext);
                instance.releaseClientConn(replyTo, client);
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
                tc1.setType(TravelCommandType.SYNC_TRAVEL)
                        .setTravelId(travelId)
                        .setStepId(stepId)
                        .setReply_to(replyTo)
                        .setTs(ts)
                        .setPayload(travelPayLoad)
                        .setGet_from(instance.getLocalIdx())
                        .setLocal_id(s)
                        .setSub_type(1);

                try {
                    GLogger.info("%d Send Travel Command to %d for the edges at %d",
                            instance.getLocalIdx(), s, System.nanoTime());

                    TGraphFSServer.Client client = instance.getClientConn(s);
                    client.syncTravel(tc1);
                    instance.releaseClientConn(s, client);
                } catch (TException te) {
                    GLogger.error("SyncTravelVertexWorker Error");
                }

                targets.remove(s);
                GLogger.debug("[%d] VertexBroadcastTVCallback Complete sending %d, %s not finish yet",
                        instance.getLocalIdx(), s, targets);

                if (targets.isEmpty()) { //finish all sends, send SYNC_TRAVEL_FINISH Command

                    TravelCommand tc3 = new TravelCommand();
                    tc3.setType(TravelCommandType.SYNC_TRAVEL_FINISH)
                            .setTravelId(travelId)
                            .setStepId(stepId)
                            .setGet_from(getFrom)
                            .setLocal_id(instance.getLocalIdx())
                            .setReply_to(replyTo)
                            .setTs(ts)
                            .setSub_type(0); //finish on vertex

                    try {
                        TGraphFSServer.Client client = instance.getClientConn(replyTo);
                        GLogger.info("%d Send TravelFinish to %d at %d finish on vertices",
                                instance.getLocalIdx(),
                                replyTo, System.nanoTime());
                        client.syncTravelFinish(tc3);
                        instance.releaseClientConn(replyTo, client);

                    } catch (TException e) {
                        e.printStackTrace();
                    }
                    return;
                }
            }
        }
    }
}