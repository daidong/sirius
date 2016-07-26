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
				if (broadcast.decrementAndGet() == 0) {
					broadcast_finish.signal();
				}
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
				aclient.split(src, new CollectEdgeCountersCallback(i));
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
		boolean reassign_decision = false;
		int local_score = fennel_score[inst.getLocalIdx()];
		int target = inst.getLocalIdx();
		for (int i = 0; i < inst.serverNum; i++)
			if (fennel_score[i] > local_score) {
				local_score = fennel_score[i];
				target = i;
			}

		if (target != inst.getLocalIdx())
			if (local_score - fennel_score[inst.getLocalIdx()] > 10)
				reassign_decision = true;


		/**
		 * Third Step: Medata Update
		 */
		if (reassign_decision != true) return;

		try {
			inst.getClientConn(target).reassign(src, 1);
			inst.getClientConn(inst.getEdgeLoc(bsrc, inst.serverNum)).reassign(src, 0);
		} catch (TException e) {
			e.printStackTrace();
		}

		Counters c = inst.edgecounters.get(src);
		c.plo = c.alo;
		c.pli = c.ali;
		inst.loc.remove(src);

		DBKey startKey = DBKey.MinDBKey(bsrc);
		DBKey endKey = DBKey.MaxDBKey(bsrc);
		ArrayList<KeyValue> kvs = inst.localstore.scanKV(startKey.toKey(), endKey.toKey());

		for (KeyValue kv : kvs){
			DBKey key = new DBKey(kv.getKey());
			ByteBuffer bdst = ByteBuffer.wrap(key.dst);
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
