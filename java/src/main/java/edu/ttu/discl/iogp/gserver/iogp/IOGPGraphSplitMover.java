package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.Movement;
import edu.ttu.discl.iogp.thrift.RedirectException;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IOGPGraphSplitMover{


	public class Task{
		int type;
		Object payload;
		public Task(ArrayList<KeyValue> payload){
			this.type = 1;
			this.payload = (Object) payload;
		}
	}

	class SplitBroadCastCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.syncTravel_call>{
		int finished;

		public SplitBroadCastCallback(int f) { finished = f; }

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
	LinkedBlockingQueue<Task> taskQueues[] = new LinkedBlockingQueue[inst.serverNum];
	AtomicInteger broadcast;
	final Lock lock = new ReentrantLock();
	final Condition broadcast_finish  = lock.newCondition();

	public IOGPGraphSplitMover(IOGPSrv instance){
		this.inst = instance;
		for (int i = 0; i < inst.serverNum; i++){
			taskQueues[i] = new LinkedBlockingQueue<>();
			new Thread(new Worker(i, taskQueues[i], inst)).run();
		}
		broadcast = new AtomicInteger(inst.serverNum);
	}

	public void splitVertex(ByteBuffer src) throws InterruptedException{
		byte[] bsrc = NIOHelper.getActiveArray(src);

		for (int i = 0; i < inst.serverNum; i++){
			TGraphFSServer.AsyncClient aclient = inst.getAsyncClientConnWithPool(i);
			try {
				aclient.split(src, new SplitBroadCastCallback(i));
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

		DBKey startKey = DBKey.MinDBKey(bsrc);
		DBKey endKey = DBKey.MaxDBKey(bsrc);
		ArrayList<KeyValue> kvs = inst.localstore.scanKV(startKey.toKey(), endKey.toKey());
		HashMap<Integer, ArrayList<KeyValue>> loadsOnEachServer = new HashMap<>();

		for (KeyValue kv : kvs){
			DBKey key = new DBKey(kv.getKey());
			int hash_target = inst.getEdgeLoc(key.dst, inst.serverNum);
			if (!loadsOnEachServer.containsKey(hash_target))
				loadsOnEachServer.put(hash_target, new ArrayList<KeyValue>());
			loadsOnEachServer.get(hash_target).add(kv);
		}
		for (int target : loadsOnEachServer.keySet()){
			Task t = new Task(loadsOnEachServer.get(target));
			taskQueues[target].put(t);
		}

	}

	public void addDataMovementTask(int i, ArrayList<KeyValue> kv) throws InterruptedException {
		Task t = new Task(kv);
		this.taskQueues[i].put(t);
	}

	class Worker implements Runnable{
		public int target;
		LinkedBlockingQueue<Task> tasks;
		IOGPSrv inst;

		public Worker(int t, LinkedBlockingQueue<Task> q, IOGPSrv inst){
			target = t;
			tasks = q;
			inst = inst;
		}
		@Override
		public void run() {
			while(true){

				try {
					Task task = tasks.take();

					if (task.type == 1) { //move data
						ArrayList<KeyValue> kvs = (ArrayList<KeyValue>) task.payload;

						try{
							inst.getClientConn(target).batch_insert(kvs, 0);
						} catch (RedirectException e) {

							int status = e.getStatus();
							for (Movement m : e.getRe()){
								ArrayList<KeyValue> payload = new ArrayList<>();
								payload.add(m.getKv());

								try {
									inst.spliter.addDataMovementTask(m.getLoc(),payload);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}
						} catch (TException e) {
							e.printStackTrace();
						}

						inst.size -= kvs.size();
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}
}
