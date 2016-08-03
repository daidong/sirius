package edu.ttu.discl.iogp.simulator.partalgs;

import edu.ttu.discl.iogp.simulator.tools.Edge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by daidong on 5/30/16.
 */
public class ThreePhase {

	public static int MAX_EDGES = 50000;
	public static int MAX_REASSIGN = 1;

	public HashMap<Integer, Integer> loc = new HashMap<>();
	public HashMap<Integer, Boolean> split = new HashMap<>();
	public HashMap<Integer, Integer> ra = new HashMap<>();

	public HashMap<Integer, HashSet<Edge>> v = new HashMap<>();

	public ArrayList<ThreePhase> cluster;

	public int serverNumber;
	public int index;

	public ThreePhase(int index, ArrayList<ThreePhase> cluster, int num){
		this.cluster = cluster;
		this.index = index;
		this.serverNumber = num;
	}

	public int hash(int vid){
		return vid % serverNumber;
	}

	//insertV is always correct for the first time
	public void insertV(int vid){
		this.v.put(vid, new HashSet<Edge>());

		this.ra.put(vid, 1);
		this.loc.put(vid, index);
		this.split.put(vid, Boolean.FALSE);
	}


	// return 0 -> success
	// return 1 -> re-insert, vertex has been split
	// return < 0 -> re-insert, vertex has been reassign.
	public int insertE(int src, int dst){
		Edge newEdge = new Edge(src, dst);
		int hash_src = hash(src);

		// vertex has been split,
		if (cluster.get(hash_src).split.get(src) == true){
			// request wrong server
			if (hash(dst) != this.index)
				return 1;
			else {
				if (!v.containsKey(src)) v.put(src, new HashSet<Edge>());
				v.get(src).add(newEdge);
				return 0;
			}
		}


		// vertex has been moved, but client still requests the old location
		if (cluster.get(hash_src).loc.get(src) != this.index){
			return (-1 - cluster.get(hash_src).loc.get(src));
		} else {
			v.get(src).add(newEdge);
		}

		//System.out.println("vertex: " + src + " dst: " + dst + " sever: " + this.index);
		// We may reassign vertices
		if (v.get(src).size() >= (MAX_REASSIGN * cluster.get(hash_src).ra.get(src))){

			cluster.get(hash_src).ra.put(src, cluster.get(hash_src).ra.get(src) * 2);

			int from = cluster.get(hash_src).loc.get(src);
			HashSet<Edge> neighbors = cluster.get(from).v.get(src);
			int max_server = from;
			int fennel_score = Integer.MAX_VALUE;

			//fennel actually check the (total_size - connected size)
			for (ThreePhase part : cluster) {
				int local_fennel = 0;
				Set<Integer> vertices = part.v.keySet();
				Set<Integer> set1 = new HashSet<>();

				for (Edge temp : neighbors)
					if (vertices.contains(temp.dst))
						set1.add(temp.dst);
				local_fennel = part.v.size() - set1.size();

				if (local_fennel < fennel_score) {
					max_server = part.index;
					fennel_score = local_fennel;
				}
			}

			if (max_server != from){
				HashSet<Edge> mov_edges = cluster.get(from).v.get(src);
				cluster.get(from).v.remove(src);

				cluster.get(hash_src).loc.put(src, max_server);
				cluster.get(max_server).v.put(src, mov_edges);
				//System.out.println("Reassign Vertex: " + src + " from " + from + " to " + max_server);
				return (-1 - max_server);
			}
		}

		// do we need split?
		if (v.get(src).size()> MAX_EDGES){
			cluster.get(hash_src).split.put(src, true);

			HashSet<Edge> all_edges = this.v.get(src);
			ArrayList<Edge> rms = new ArrayList<>();

			for (Edge edge : all_edges){
				if (hash(edge.dst) != index){
					rms.add(edge);
				}
			}

			for (Edge edge : rms){
				ThreePhase target = cluster.get(hash(edge.dst));
				target.insertE(edge.src, edge.dst);
				this.v.get(src).remove(edge);
			}

			// move vertex back to its initial place
			cluster.get(hash_src).loc.put(src, hash_src);

			return 1;
		}

		return 0;
	}

	public static void workload_run_threshold(HashSet<Edge> edges, int thresdhold){
		ThreePhase.MAX_REASSIGN = thresdhold;
		workload_run(edges, 32);
	}

	public static void workload_run(HashSet<Edge> edges, int cluster_size){

		int total_cut = 0;
		int total_reassign = 0;
		int highest_out_degree = 0;
		int highest_in_degree = 0;

		//System.out.println("Insert generated graph into ParitionAlgorithm.ThreePhase algorithm");

		ArrayList<Integer> insertedV = new ArrayList<>();
		ArrayList<Integer> splitV = new ArrayList<>();
		HashMap<Integer, Integer> locations = new HashMap<>();

		ArrayList<ThreePhase> cluster = new ArrayList<>();
		for (int i = 0; i < cluster_size; i++) {
			cluster.add(i, new ThreePhase(i, cluster, cluster_size));
		}

		ArrayList<Edge> visitedEdges = new ArrayList<>();

		for (Edge e : edges) {
			if (e.src == e.dst) continue;

			visitedEdges.add(e);

			if (!insertedV.contains(e.src)) {
				cluster.get(e.src % cluster_size).insertV(e.src);
				insertedV.add(e.src);
			}
			if (!insertedV.contains(e.dst)) {
				cluster.get(e.dst % cluster_size).insertV(e.dst);
				insertedV.add(e.dst);
			}

			int rtn = 0;

			if (splitV.contains(e.src))
				rtn = cluster.get(e.dst % cluster_size).insertE(e.src, e.dst);
			else if (locations.containsKey(e.src))
				rtn = cluster.get(locations.get(e.src)).insertE(e.src, e.dst);
			else
				rtn = cluster.get(e.src % cluster_size).insertE(e.src, e.dst);

			if (rtn < 0){
				int target = -1 - rtn;
				cluster.get(target).insertE(e.src, e.dst);
				locations.put(e.src, target);
				total_reassign += 1;
			}

			if (rtn == 1){
				splitV.add(e.src);
				cluster.get(e.dst % cluster_size).insertE(e.src, e.dst);
			}


			if (visitedEdges.size() % 100000 == 1) {
				total_cut = 0;
				for (Edge eval : visitedEdges) {
					int src = eval.src;
					int dst = eval.dst;

					if (cluster.get(src % cluster_size).loc.get(src) !=
							cluster.get(dst % cluster_size).loc.get(dst))
						total_cut++;
				}
				System.out.println("Cuts: " + total_cut +
						" Percent: " + (float) total_cut / (float) visitedEdges.size());
			}

		}

		total_cut = 0;
		for (Edge eval : edges) {
			int src = eval.src;
			int dst = eval.dst;

			if (cluster.get(src % cluster_size).loc.get(src) !=
					cluster.get(dst % cluster_size).loc.get(dst))
				total_cut++;
		}
		System.out.println("Total Cuts: " + total_cut + " Reassign: " + total_reassign +
				" Percent: " + (float) total_cut / (float) edges.size());
	}
}
