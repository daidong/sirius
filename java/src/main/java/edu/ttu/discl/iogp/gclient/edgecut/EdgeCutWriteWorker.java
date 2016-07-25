package edu.ttu.discl.iogp.gclient.edgecut;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.tengine.travel.GTravel;
import edu.ttu.discl.iogp.utils.ArrayPrimitives;
import edu.ttu.discl.iogp.utils.RMATGraphGenerator;
import org.apache.thrift.TException;
import java.util.ArrayList;

public class EdgeCutWriteWorker implements Runnable{

	String[] args;
	RMATGraphGenerator generator;
	int idx;
	int threads;
	public EdgeCutWriteWorker(String[] params, RMATGraphGenerator g, int i, int t){
		this.args = params;
		this.generator = g;
		this.idx = i;
		this.threads = t;
	}
	@Override
	public void run() {
		int port = Integer.parseInt(args[0]);
		int testType = Integer.parseInt(args[1]);
		
		ArrayList<String> allSrvs = new ArrayList<>();
		for (int i = 4; i < args.length; i++)
			allSrvs.add(args[i]);

		EdgeCutClt ecc = new EdgeCutClt(port, allSrvs);

		if (testType == 0) { //insert test
			
			String payload256 = "";
			for (int i = 0; i < 128; i++){
				payload256 += "a";
			}
			byte[] vals = (payload256).getBytes();
			long start = System.currentTimeMillis();
			
			int totalSize = generator.generated.size();
			int slice = totalSize / this.threads ;
			
			for (int index = slice * this.idx ; index < slice * (this.idx + 1) && index < totalSize; index ++){
				RMATGraphGenerator.Edge e = generator.generated.get(index);
				if (e.src != e.dst){
					try {
						ecc.insert(
								("vertex"+(e.src)).getBytes(), 
								EdgeType.IN,
								("vertex"+(e.dst)).getBytes(), 
								vals);
					} catch (TException e1) {
						e1.printStackTrace();
					}
				}
			}
			System.out.println("Insert time: " + (System.currentTimeMillis() - start));
		} 

		if (testType == 3){
			String payload256 = "";
			for (int i = 0; i < 128; i++){
				payload256 += "a";
			}
			byte[] vals = (payload256).getBytes();
			long start = System.currentTimeMillis();
			
			int totalSize = generator.generated.size();
			int slice = totalSize / this.threads ;
			
			for (int index = slice * this.idx ; index < slice * (this.idx + 1) && index < totalSize; index ++){
				try {
					ecc.insert(
							("vertex"+(0)).getBytes(), 
							EdgeType.IN,
							("vertex"+(index)).getBytes(), 
							vals);
				} catch (TException e1) {
					e1.printStackTrace();
				}
			}
			System.out.println("Insert time: " + (System.currentTimeMillis() - start));
		}
		
		if (testType == 1){ //2-step sync traversal.

			GTravel gt = new GTravel();
			int edge = EdgeType.IN.get();
			byte[] bEdge = ArrayPrimitives.itob(edge);

			gt.v(("vertex0").getBytes())
			.et(bEdge).next()
			.et(bEdge).next()
			.et(bEdge).next()
			.et(bEdge).next()
			.v();
			try {
				ecc.submitSyncTravel(gt.plan());
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} 

		if (testType == 2){ 

			GTravel gt = new GTravel();
			int edge = EdgeType.IN.get();
			byte[] bEdge = ArrayPrimitives.itob(edge);

			gt.v(("vertex0").getBytes())
			.et(bEdge).next()
			.et(bEdge).next()
			.et(bEdge).next()
			.et(bEdge).next()
			.v();
			try {
				ecc.submitTravel(gt.plan());
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

}
