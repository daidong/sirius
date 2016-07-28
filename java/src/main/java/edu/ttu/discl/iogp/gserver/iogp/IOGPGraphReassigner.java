package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.Constants;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reassigner take src as input and take charge all reassignments.
 */
public class IOGPGraphReassigner {

	class CollectEdgeCountersCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.fennel_call> {
		ByteBuffer src;
		int finished;

		public CollectEdgeCountersCallback(ByteBuffer s, int f) { src = s; finished = f; }

		@Override
		public void onComplete(TGraphFSServer.AsyncClient.fennel_call t) {
			AtomicInteger broadcast = broadcasts.get(src);
			final Condition broadcast_finish = broadcast_finishes.get(src);

			//GLogger.info("[%d] In Fennel Callback broadcast for %s, broadcast: %d",
			//		inst.getLocalIdx(), new String(NIOHelper.getActiveArray(src)), broadcast.get());

			lock.lock();
			try {
				fennel_score[finished] = t.getResult();
				if (broadcast.decrementAndGet() == 0) {
					broadcast_finish.signal();
				}
			} catch (TException e) {
				e.printStackTrace();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void onError(Exception e) {
			GLogger.info("[%d] On Error", inst.getLocalIdx());
		}
	}

	public IOGPSrv inst;
	final Lock lock = new ReentrantLock();
	ConcurrentHashMap<ByteBuffer, AtomicInteger> broadcasts;
	ConcurrentHashMap<ByteBuffer, Condition> broadcast_finishes;
	int fennel_score[];


	public IOGPGraphReassigner(IOGPSrv srv){
		inst = srv;
		broadcasts = new ConcurrentHashMap<>();
		broadcast_finishes = new ConcurrentHashMap<>();
		fennel_score = new int[inst.serverNum];
		for (int i = 0; i < inst.serverNum; i++) fennel_score[i] = 0;
	}

	public void reassignVertex(ByteBuffer src) throws InterruptedException {

		/**
		 * First Step: Collect all edge counters
		 */
		byte[] bsrc = NIOHelper.getActiveArray(src);

		boolean jump = false;
		broadcasts.putIfAbsent(src, new AtomicInteger(inst.serverNum));
		AtomicInteger broadcast = broadcasts.get(src);
		broadcast_finishes.putIfAbsent(src, lock.newCondition());
		final Condition broadcast_finish = broadcast_finishes.get(src);

		for (int i = 0; i < inst.serverNum; i++){
			TGraphFSServer.AsyncClient aclient = inst.getAsyncClientConnWithPool(i);
			try {
				if (i != inst.getLocalIdx()) {
					aclient.fennel(src, new CollectEdgeCountersCallback(src, i));
				} else {
					fennel_score[i] = (0 - inst.size.get());
					if (broadcast.decrementAndGet() == 0) {
						jump = true;
					}
				}
			} catch (TException e) {
				e.printStackTrace();
			}
		}

		if (!jump) {
			lock.lock();
			try {
				broadcast_finish.await();
			} finally {
				lock.unlock();
			}
		}

		GLogger.info("[%d] broadcast and get fennel score from all servers", inst.getLocalIdx());
		broadcasts.remove(src);
		broadcast_finishes.remove(src);

		/**
		 * Second Step: Choose the best one and decide whether move the vertices or not
		 */
		inst.edgecounters.putIfAbsent(src, new Counters());
		Counters c = inst.edgecounters.get(src);

		boolean reassign_decision = false;
		int local_score = 2 * (c.pli + c.plo) - inst.size.get();
		int max_score = local_score;

		int target = inst.getLocalIdx();
		for (int i = 0; i < inst.serverNum; i++)
			if (fennel_score[i] > max_score) {
				max_score = fennel_score[i];
				target = i;
			}

		if (target != inst.getLocalIdx())
			if (max_score - local_score > 10)
				reassign_decision = true;

		/**
		 * Third Step: Medata Update
		 */
		if (reassign_decision != true) return;
		GLogger.info("[%d] %s should move from %d to %d for %d times",
				inst.getLocalIdx(), new String(bsrc),
				inst.getLocalIdx(), target, c.reassign_times);

		try {
			/**
			 * The order is important. First setup target, then update the hash sorce
			 */
			int hash_loc = inst.getEdgeLoc(bsrc, inst.serverNum);
			// to the target server, "type" is how many this vertex has been reassigned.
			if (c.reassign_times < 1) GLogger.error("c.reassign_times should never small than 1");

			TGraphFSServer.Client targetClient = inst.getClientConn(target);
			synchronized (targetClient) {
				targetClient.reassign(src, c.reassign_times, target);
			}

			TGraphFSServer.Client hashClient = inst.getClientConn(hash_loc);
			synchronized (hashClient) {
				if (hash_loc != inst.getLocalIdx())
					hashClient.reassign(src, 0, target);
				else
					inst.handler.reassign(src, 0, target);
			}
		} catch (TException e) {
			e.printStackTrace();
		}

		c.plo = c.alo;
		c.pli = c.ali;
		inst.loc.remove(src);

		DBKey startKey = DBKey.MinDBKey(bsrc);
		DBKey endKey = DBKey.MaxDBKey(bsrc);
		ArrayList<KeyValue> kvs = inst.localstore.scanKV(startKey.toKey(), endKey.toKey());
		inst.size.addAndGet(0 - kvs.size());

		for (KeyValue kv : kvs){
			DBKey key = new DBKey(kv.getKey());
			ByteBuffer bdst = ByteBuffer.wrap(key.dst);
			if (!inst.edgecounters.containsKey(bdst))
				inst.edgecounters.put(bdst, new Counters());
			Counters dstc = inst.edgecounters.get(bdst);

			if (inst.loc.containsKey(bdst)
					&& inst.loc.get(bdst) == inst.getLocalIdx()){
				if (key.type == EdgeType.OUT.get())
					dstc.ali -= 1;
				if (key.type == EdgeType.IN.get())
					dstc.alo -= 1;
			}
		}

		/**
		 * Fourth Step: Conduct data movement
		 */
		try {
			TGraphFSServer.Client client = inst.getClientConn(target);
			synchronized (client) {
				//GLogger.info("[%d]-[Reassign]-[%d] Client: %s calls batch_insert",
				//		inst.getLocalIdx(), target, client.toString());
				client.batch_insert(kvs, 1);
			}

		} catch (TException e) {
			e.printStackTrace();
		}

	}
}
