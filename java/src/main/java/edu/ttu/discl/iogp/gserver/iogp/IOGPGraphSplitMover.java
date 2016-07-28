package edu.ttu.discl.iogp.gserver.iogp;

import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.thrift.KeyValue;
import edu.ttu.discl.iogp.thrift.Movement;
import edu.ttu.discl.iogp.thrift.RedirectException;
import edu.ttu.discl.iogp.thrift.TGraphFSServer;
import edu.ttu.discl.iogp.utils.GLogger;
import edu.ttu.discl.iogp.utils.NIOHelper;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
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

	class SplitBroadCastCallback implements AsyncMethodCallback<TGraphFSServer.AsyncClient.split_call>{
		int finished;
		ByteBuffer src;

		public SplitBroadCastCallback(ByteBuffer s, int f) { src = s; finished = f; }

		@Override
		public void onComplete(TGraphFSServer.AsyncClient.split_call t) {
			AtomicInteger broadcast = broadcasts.get(src);
			final Condition broadcast_finish = broadcast_finishes.get(src);
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
	LinkedBlockingQueue<Task> taskQueues[];
	final Lock lock = new ReentrantLock();
	ConcurrentHashMap<ByteBuffer, AtomicInteger> broadcasts;
	ConcurrentHashMap<ByteBuffer, Condition> broadcast_finishes;

	public IOGPGraphSplitMover(IOGPSrv instance){
		this.inst = instance;
		taskQueues = new LinkedBlockingQueue[inst.serverNum];
		broadcasts = new ConcurrentHashMap<>();
		broadcast_finishes = new ConcurrentHashMap<>();
	}

	public void startWorkers(){
		for (int i = 0; i < inst.serverNum; i++){
			taskQueues[i] = new LinkedBlockingQueue<>();
			new Thread(new Worker(i, taskQueues[i], inst)).start();
		}
	}

	public void splitVertex(ByteBuffer src) throws InterruptedException{
		byte[] bsrc = NIOHelper.getActiveArray(src);
		boolean jump = false;

		broadcasts.putIfAbsent(src, new AtomicInteger(inst.serverNum));
		AtomicInteger broadcast = broadcasts.get(src);
		broadcast_finishes.putIfAbsent(src, lock.newCondition());
		final Condition broadcast_finish = broadcast_finishes.get(src);

		for (int i = 0; i < inst.serverNum; i++){
			TGraphFSServer.AsyncClient aclient = inst.getAsyncClientConnWithPool(i);
			try {
				if (i != inst.getLocalIdx())
					aclient.split(src, new SplitBroadCastCallback(src, i));
				else {
					inst.handler.split(src);
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

		GLogger.info("[%d] broadcast and get split done from all servers", inst.getLocalIdx());
		broadcasts.remove(src);
		broadcast_finishes.remove(src);

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
			addDataMovementTask(target, loadsOnEachServer.get(target));
		}
	}

	public void addDataMovementTask(int i, ArrayList<KeyValue> kv) throws InterruptedException {
		if (i != inst.getLocalIdx()) {
			Task t = new Task(kv);
			this.taskQueues[i].put(t);
		}
	}

	class Worker implements Runnable{
		public int target;
		LinkedBlockingQueue<Task> tasks;
		IOGPSrv inst;

		public Worker(int t, LinkedBlockingQueue<Task> q, IOGPSrv inst){
			this.target = t;
			this.tasks = q;
			this.inst = inst;
		}
		@Override
		public void run() {
			while(true){

				try {
					Task task = tasks.take();

					if (task.type == 1) { //move data
						ArrayList<KeyValue> kvs = (ArrayList<KeyValue>) task.payload;
						if (kvs != null) {
							try {
								TGraphFSServer.Client client = inst.getClientConn(this.target);
								synchronized (client){
								//	GLogger.info("[%d]-[Split]-[%d] Client: %s calls batch_insert",
								//			inst.getLocalIdx(), target, client.toString());
									client.batch_insert(kvs, 0);
								}
 							} catch (RedirectException e) {

								int status = e.getStatus();
								for (Movement m : e.getRe()) {
									ArrayList<KeyValue> payload = new ArrayList<>();
									payload.add(m.getKv());

									try {
										inst.spliter.addDataMovementTask(m.getLoc(), payload);
									} catch (InterruptedException e1) {
										e1.printStackTrace();
									}
								}
								inst.size.addAndGet(e.getReSize() - kvs.size());

							} catch (TException e) {
								e.printStackTrace();
							}
						}
					}

				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}
	}
}
