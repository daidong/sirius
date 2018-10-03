package edu.dair.sgdb.simulator.partalgs;

import edu.dair.sgdb.simulator.tools.Edge;

import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by daidong on 7/5/16.
 */
public class EdgeCutHashing {

    public static void workload_run(HashSet<Edge> edges, int cluster_size) {

        int total_cut = 0;
        int highest_out_degree = 0;
        int highest_in_degree = 0;

        HashMap<Integer, Integer> degrees = new HashMap<>();

        for (Edge e : edges) {
            if (!degrees.containsKey(e.src)) {
                degrees.put(e.src, 0);
            }
            degrees.put(e.src, degrees.get(e.src) + 1);
            if (e.src % cluster_size != e.dst % cluster_size)
                total_cut++;
        }
        for (int d : degrees.values())
            if (d > highest_out_degree)
                highest_out_degree = d;

        System.out.println("Hash Total Cuts: " + total_cut + " Percent: " + (float) total_cut / (float) edges.size());

    }
}
