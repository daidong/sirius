package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reassigner take src as input and take charge all reassignments.
 */
public class IOGPGraphReassigner {

	class CollectEdgeCountersCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.syncTravel_call> {
		int finished;

		public CollectEdgeCountersCallback(int f) { finished = f; }

		@Override
		public void onComplete(TGraphFSServer.AsyncClient.syncTravel_call t) {
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

		}
	}

	public IOGPSrv inst;
	AtomicInteger broadcast;
	final Lock lock = new ReentrantLock();
	final Condition broadcast_finish  = lock.newCondition();
	int fennel_score[];


	public IOGPGraphReassigner(IOGPSrv srv){
		inst = srv;
		broadcast = new AtomicInteger(inst.serverNum);
		fennel_score = new int[inst.serverNum];
		for (int i = 0; i < inst.serverNum; i++) fennel_score[i] = 0;
	}

	public void reassignVertex(ByteBuffer src) throws InterruptedException {

		/**
		 * First Step: Collect all edge counters
		 */
		byte[] bsrc = NIOHelper.getActiveArray(src);

		for (int i = 0; i < inst.serverNum; i++){
			TGraphFSServer.AsyncClient aclient = inst.getAsyncClientConnWithPool(i);
			try {
				aclient.fennel(src, new CollectEdgeCountersCallback(i));
			} catch (TException e) {
				e.printStackTrace();
			}
		}

		lock.lock();
		try {
			broadcast_finish.await();
		} finally {
			lock.unlock();
		}

		/**
		 * Second Step: Choose the best one and decide whether move the vertices or not
		 */
		if (!inst.edgecounters.containsKey(src))
			inst.edgecounters.put(src, new Counters(IOGPHandler.REASSIGN_THRESHOLD));
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

		try {
			/**
			 * The order is important. First setup target, then update the hash sorce
			 */
			inst.getClientConn(target).reassign(src, 1, target);
			inst.getClientConn(inst.getEdgeLoc(bsrc, inst.serverNum)).reassign(src, 0, target);
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
				inst.edgecounters.put(bdst, new Counters(IOGPHandler.REASSIGN_THRESHOLD));
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
			inst.getClientConn(target).batch_insert(kvs, 1);
		} catch (TException e) {
			e.printStackTrace();
		}

	}
}
