package edu.dair.sgdb.simulator.partalgs;

import edu.dair.sgdb.simulator.tools.Edge;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

public class Fennel {

    TreeMap<Integer, ArrayList<Integer>> csr;
    TIntObjectHashMap<TIntHashSet> csr_s;
    TIntObjectHashMap<TIntHashSet> reverse_csr_s;

    HashSet<Edge> edges;
    int cluster_size;

    TIntHashSet[] parts;
    HashMap<Integer, Integer> location = new HashMap<>();

    public Fennel(TreeMap<Integer, ArrayList<Integer>> inputs, HashSet<Edge> edges, int size) {
        this.csr = inputs;
        this.edges = edges;
        this.cluster_size = size;
        this.parts = new TIntHashSet[cluster_size];
        for (int i = 0; i < cluster_size; i++)
            parts[i] = new TIntHashSet();
    }

    // Save memory constructor
    public Fennel(TIntObjectHashMap<TIntHashSet> csr,
                  TIntObjectHashMap<TIntHashSet> reverse_csr,
                  HashSet<Edge> generated, int size) {
        this.csr_s = csr;
        this.reverse_csr_s = reverse_csr;
        this.edges = generated;
        this.cluster_size = size;
        this.parts = new TIntHashSet[cluster_size];
        for (int i = 0; i < cluster_size; i++)
            parts[i] = new TIntHashSet();
    }

    // Save Memory workload run
    public void workload_run_s() {
        int multiple = 10;
        while (multiple-- > 0) {

            TIntObjectIterator<TIntHashSet> iterator = this.csr_s.iterator();
            while (iterator.hasNext()) {
                iterator.advance();
                int key = iterator.key();
                TIntHashSet value = iterator.value();
                value.addAll(this.reverse_csr_s.get(key));

                int target = 0;
                int score = Integer.MAX_VALUE;
                for (int i = 0; i < cluster_size; i++) {
                    int overlap_size = 0;

                    TIntIterator iter = value.iterator();
                    while (iter.hasNext()) {
                        int v = iter.next();
                        if (parts[i].contains(v))
                            overlap_size++;
                    }

                    int fennel_score = (parts[i].size() - overlap_size);
                    if (fennel_score < score) {
                        score = fennel_score;
                        target = i;
                    }
                }
                if (location.containsKey(key))
                    parts[location.get(key)].remove(key);
                parts[target].add(key);
                location.put(key, target);
            }


            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;

            for (int i = 0; i < cluster_size; i++) {
                if (parts[i].size() < min) min = parts[i].size();
                if (parts[i].size() > max) max = parts[i].size();
            }

            System.out.println("DIFFERENCE: " + (max - min));

            int total_cut = 0;
            for (Edge e : edges) {
                int src = e.src;
                int dst = e.dst;

                if (!location.containsKey(src) || !location.containsKey(dst)) continue;
                int src_loc = location.get(src);
                int dst_loc = location.get(dst);

                if (src_loc != dst_loc) {
                    total_cut += 1;
                }
            }
            System.out.println("Fennel Cuts: " + total_cut +
                    " Percent: " + (float) total_cut / (float) edges.size());
        }
    }


    public void workload_run() {
        for (int key : csr.keySet()) {
            int target = 0;
            int score = Integer.MAX_VALUE;
            for (int i = 0; i < cluster_size; i++) {
                Set<Integer> set = new HashSet<>();

                for (int v : csr.get(key))
                    if (parts[i].contains((Integer) v))
                        set.add(v);

                int fennel_score = (parts[i].size() - set.size());

                if (fennel_score < score) {
                    score = fennel_score;
                    target = i;
                }
            }
            //System.out.println("Vertex: " + key + " is assigned to " + target);
            parts[target].add(key);
            location.put(key, target);
        }

        for (int i = 0; i < cluster_size; i++)
            System.out.println("part[" + i + "]'s size: " + parts[i].size());

        int total_cut = 0;
        for (Edge e : edges) {
            int src = e.src;
            int dst = e.dst;

            int src_loc = location.get(src);
            int dst_loc = location.get(dst);

            if (src_loc != dst_loc) {
                total_cut += 1;
            }
        }
        System.out.println("ParitionAlgorithm.Fennel Total Cuts: " + total_cut + " Percent: " + (float) total_cut / (float) edges.size());

    }
}
