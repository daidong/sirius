package edu.dair.sgdb.tengine.async;

import edu.dair.sgdb.gserver.AbstractSrv;
import edu.dair.sgdb.tengine.TravelLocalReader;
import edu.dair.sgdb.tengine.travel.JSONCommand;
import edu.dair.sgdb.tengine.travel.SingleStep;
import edu.dair.sgdb.thrift.*;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;
import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import static edu.dair.sgdb.utils.NIOHelper.getActiveArray;

public class AsyncTravelWorker implements Runnable {

    AbstractSrv instance;
    AsyncTravelEngine engine;

    public AsyncTravelWorker(AbstractSrv s, AsyncTravelEngine e) {
        this.instance = s;
        this.engine = e;
    }

    private ArrayList<SingleStep> buildTravelFromJSON(JSONArray payload) {
        ArrayList<SingleStep> travelPlan = new ArrayList<>();
        for (int i = 0; i < payload.size(); i++) {
            JSONObject idx = (JSONObject) payload.get(i);
            JSONObject obj = (JSONObject) idx.get("value");
            SingleStep ss = SingleStep.parseJSON(obj.toString());
            travelPlan.add(ss);
        }
        return travelPlan;
    }

    private void callRegWithRetry(int sid, TravelCommand tc, UUID id) {
        GLogger.info("S TE %d %d %d", instance.getLocalIdx(), sid, System.nanoTime());
        int repeated = 0;
        while (repeated < Constants.RETRY) {
            try {
                TGraphFSServer.Client client = instance.getClientConn(sid);
                synchronized (client) {
                    client.travelReg(tc);
                }
                instance.releaseClientConn(sid, client);
                repeated = Constants.RETRY;
            } catch (TException e) {
                repeated += 1;
            }
        }
    }

    private void callFinWithRetry(int sid, TravelCommand tc, UUID id) {
        GLogger.info("S TF %d %d %d", instance.getLocalIdx(), sid, System.nanoTime());
        int repeated = 0;
        while (repeated < Constants.RETRY) {
            try {
                TGraphFSServer.Client client = instance.getClientConn(sid);
                synchronized (client) {
                    client.travelFin(tc);
                }
                instance.releaseClientConn(sid, client);
                repeated = Constants.RETRY;
            } catch (TException e) {
                repeated += 1;
            }
        }
    }

    private void callRtnWithRetry(int sid, TravelCommand tc, UUID id) {
        GLogger.info("S TR %d %d %d", instance.getLocalIdx(), sid, System.nanoTime());
        int repeated = 0;
        while (repeated < Constants.RETRY) {
            try {
                TGraphFSServer.Client client = instance.getClientConn(sid);
                synchronized (client) {
                    client.travelRtn(tc);
                }
                instance.releaseClientConn(sid, client);
                repeated = Constants.RETRY;
            } catch (TException e) {
                repeated += 1;
            }
        }
    }

    private void callTravelWithRetry(int sid, TravelCommand tc, UUID id) {
        GLogger.info("S TV %d %d %d %d", instance.getLocalIdx(), sid, System.nanoTime(), 1);
        int repeated = 0;
        while (repeated < Constants.RETRY) {
            try {
                if (sid == instance.getLocalIdx()) { //local data
                    if (tc.getSub_type() == 0) {
                        engine.incrEdge2DstLocalCounter(tc.getTravelId(), tc.getKeys().size());
                    }
                    engine.travel(tc);
                } else {
                    TGraphFSServer.Client client = instance.getClientConn(sid);
                    synchronized (client) {
                        client.travel(tc);
                    }
                    instance.releaseClientConn(sid, client);
                }
                repeated = Constants.RETRY;
            } catch (TException e) {
                repeated += 1;
            }
        }
    }

    @Override
    public void run() {

        while (true) {
            UUID id = UUID.randomUUID();

            long travelId = 0;
            try {
                travelId = engine.travels.take();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            AsyncTravelStatus ats = engine.travel_status.get(travelId);
            Entry<Integer, AsyncTravelStatus.StepPayLoad> poll = null;
            ConcurrentSkipListMap<Integer, AsyncTravelStatus.StepPayLoad> currTravel = null;

            synchronized (ats) {
                currTravel = ats.getTravelVertices();
                poll = currTravel.pollFirstEntry();

            }

            synchronized (engine.travels) {
                if (!currTravel.isEmpty() && !engine.travels.contains(travelId)) {
                    engine.travels.offerFirst(travelId);
                }
            }

            if (poll == null) {
                continue;
            }

            int stepId = poll.getKey();
            AsyncTravelStatus.StepPayLoad load = poll.getValue();
            HashSet<AsyncTravelStatus.PVertex> pKeys = load.vertices;
            HashMap<Integer, Integer> epochMap = load.epochIdPerServer;

            GLogger.debug("[%d] Travel Worker Process, Local Request %d keys, from server: %s with epoch id: %s",
                    instance.getLocalIdx(), pKeys.size(), epochMap.keySet(), epochMap.values());

            boolean lastStep = false;
            int replyTo = (int) engine.getTravelContents(travelId).get("reply_to");
            long ts = (long) engine.getTravelContents(travelId).get("ts");
            JSONArray payload = (JSONArray) engine.getTravelContents(travelId).get("travel_payload");
            ArrayList<SingleStep> travelPlan = buildTravelFromJSON(payload);
            SingleStep currStep = travelPlan.get(stepId);
            if (stepId == (travelPlan.size() - 1)) {
                lastStep = true;
            }

            // Divide vertex/Edge processing
            HashSet<ByteBuffer> checkVertices = new HashSet<>();
            HashSet<ByteBuffer> checkEdges = new HashSet<>();
            for (AsyncTravelStatus.PVertex pv : pKeys) {
                if (pv.p_ve == (byte) 0) {
                    checkVertices.add(pv.key);
                } else {
                    checkEdges.add(pv.key);
                }
            }

            if (!checkVertices.isEmpty()) {
                int tid = engine.mid.addAndGet(1);
                GLogger.info("S VR %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());
                ArrayList<byte[]> passedVertices = TravelLocalReader.filterVertices(this.instance.localStore,
                        checkVertices,
                        currStep, ts);
                GLogger.info("R VR %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());

                GLogger.debug("[%d] check vertices %d and pass: %d", instance.getLocalIdx(),
                        checkVertices.size(),
                        passedVertices.size());

                if (lastStep) {
                    ArrayList<KeyValue> vals = new ArrayList<>();
                    for (byte[] vertex : passedVertices) {
                        KeyValue kv = new KeyValue();
                        kv.setKey(vertex);
                        vals.add(kv);
                    }

                    // return the value and also report to coordinator that process is finished here.
                    List<EpochEntity> processedEpochs = new ArrayList<>();
                    for (int server : epochMap.keySet()) {
                        EpochEntity ee = new EpochEntity();
                        ee.setServerId(server);
                        ee.setEpoch(epochMap.get(server));
                        processedEpochs.add(ee);
                    }

                    String processed = "";
                    for (EpochEntity ee : processedEpochs) {
                        processed += (ee.serverId + " --> " + this.instance.getLocalIdx() + " : " + ee.epoch + ", ");
                    }
                    GLogger.debug("[%d] Travel Worker Finish, Finish work: %s",
                            instance.getLocalIdx(), processed);

                    TravelCommand tc = new TravelCommand();
                    tc.setType(TravelCommandType.TRAVEL_RTN).setTravelId(travelId).setStepId(stepId)
                            .setReply_to(replyTo)
                            .setGet_from(instance.getLocalIdx())
                            .setVals(vals)
                            .setEpoch(processedEpochs);
                    callRtnWithRetry(replyTo, tc, id);

                } else {
                    // Already verify the vertices; We need to verify the edges before generating next-step vertices
                    HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
                    for (byte[] key : passedVertices) {
                        Set<Integer> servers = instance.getEdgeLocs(key);
                        //GLogger.debug("[%d] Get edges of [%s]: %s", instance.getLocalIdx(), new String(key), servers);
                        for (Integer s : servers) {
                            if (!perServerVertices.containsKey(s)) {
                                perServerVertices.put(s, new HashSet<ByteBuffer>());
                            }
                            perServerVertices.get(s).add(ByteBuffer.wrap(key));
                        }
                    }

                    //Tell coordinator that I started new epoch
                    int curr_epoch = engine.getEpoch();
                    List<Integer> extSrvs = new ArrayList<>(perServerVertices.keySet());
                    GLogger.debug("[%d] Travel Worker Extend (Vertex To Edges), Regs Epoch: %d --> %s. EpochId = %d", instance.getLocalIdx(),
                            instance.getLocalIdx(), extSrvs, curr_epoch);

                    TravelCommand tc1 = new TravelCommand();
                    tc1.setType(TravelCommandType.TRAVEL_REG).setTravelId(travelId).setStepId(stepId)
                            .setReply_to(replyTo).setGet_from(this.instance.getLocalIdx()).setExt_srv(extSrvs)
                            .setSub_type(1).setLocal_id(curr_epoch);
                    callRegWithRetry(replyTo, tc1, id);

                    for (int s : perServerVertices.keySet()) {
                        List<ByteBuffer> nextKeys = new ArrayList<ByteBuffer>(perServerVertices.get(s));
                        JSONCommand jc = new JSONCommand();
                        jc.add("travel_payload", payload);

                        GLogger.debug("[%d] Vertex to Edge Extend to server %d total key size: %d",
                                instance.getLocalIdx(), s, nextKeys.size());

                        TravelCommand tc = new TravelCommand();
                        tc.setType(TravelCommandType.TRAVEL).setTravelId(travelId).setStepId(stepId)
                                .setReply_to(replyTo).setGet_from(this.instance.getLocalIdx())
                                .setTs(ts).setPayload(jc.genString()).setKeys(nextKeys)
                                .setSub_type(1)
                                .setLocal_id(curr_epoch);

                        callTravelWithRetry(s, tc, id);
                    }

                }
            }

            if (!checkEdges.isEmpty()) {
                ArrayList<byte[]> edgeCheckingVertices = new ArrayList<>();
                for (ByteBuffer k : checkEdges) {
                    byte[] bk = getActiveArray(k);
                    edgeCheckingVertices.add(bk);
                }

                int tid = engine.mid.addAndGet(1);
                GLogger.info("S ER %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());
                HashSet<byte[]> nextVertices = TravelLocalReader.scanLocalEdges(this.instance.localStore,
                        edgeCheckingVertices, currStep, ts);
                GLogger.info("R ER %d %d %d", instance.getLocalIdx(), tid, System.nanoTime());

                HashMap<Integer, HashSet<ByteBuffer>> perServerVertices = new HashMap<>();
                for (byte[] v : nextVertices) {
                    Set<Integer> servers = instance.getVertexLoc(v);
                    for (int s : servers) {
                        if (!perServerVertices.containsKey(s)) {
                            perServerVertices.put(s, new HashSet<ByteBuffer>());
                        }
                        perServerVertices.get(s).add(ByteBuffer.wrap(v));
                    }
                }

                int curr_epoch = engine.getEpoch();
                List<Integer> extSrvs = new ArrayList<>(perServerVertices.keySet());
                GLogger.debug("[%d] Travel Worker Extend (Edges To Vertices), Regs Epoch: %d --> %s. EpochId = %d", instance.getLocalIdx(),
                        instance.getLocalIdx(), extSrvs, curr_epoch);

                TravelCommand tc1 = new TravelCommand();
                tc1.setType(TravelCommandType.TRAVEL_REG).setTravelId(travelId).setStepId(stepId + 1)
                        .setReply_to(replyTo).setGet_from(this.instance.getLocalIdx()).setExt_srv(extSrvs)
                        .setSub_type(0).setLocal_id(curr_epoch);
                callRegWithRetry(replyTo, tc1, id);

                for (int s : perServerVertices.keySet()) {
                    List<ByteBuffer> nextKeys = new ArrayList<ByteBuffer>(perServerVertices.get(s));
                    JSONCommand jc = new JSONCommand();
                    jc.add("travel_payload", payload);

                    GLogger.debug("[%d] Edge to Vertex Extend to server %d total key size: %d",
                            instance.getLocalIdx(), s, nextKeys.size());

                    TravelCommand tc = new TravelCommand();
                    tc.setType(TravelCommandType.TRAVEL).setTravelId(travelId)
                            .setStepId(stepId + 1).setReply_to(replyTo).setGet_from(this.instance.getLocalIdx())
                            .setTs(ts).setPayload(jc.genString())
                            .setSub_type(0).setLocal_id(curr_epoch)
                            .setKeys(nextKeys);

                    callTravelWithRetry(s, tc, id);
                }
            }

            //tell coordinator that I finished
            List<EpochEntity> processedEpochs = new ArrayList<>();
            for (int server : epochMap.keySet()) {
                EpochEntity ee = new EpochEntity();
                ee.setServerId(server);
                ee.setEpoch(epochMap.get(server));
                processedEpochs.add(ee);
            }

            String processed = "";
            for (EpochEntity ee : processedEpochs) {
                processed += (ee.serverId + " --> " + this.instance.getLocalIdx() + " : " + ee.epoch + ", ");
            }
            GLogger.debug("[%d] Travel Worker Finish, Finish work: %s",
                    instance.getLocalIdx(), processed);

            TravelCommand tc2 = new TravelCommand();
            tc2.setType(TravelCommandType.TRAVEL_FIN).setTravelId(travelId).setStepId(stepId)
                    .setReply_to(replyTo).setGet_from(this.instance.getLocalIdx())
                    .setEpoch(processedEpochs);
            callFinWithRetry(replyTo, tc2, id);
        }
    }
}
