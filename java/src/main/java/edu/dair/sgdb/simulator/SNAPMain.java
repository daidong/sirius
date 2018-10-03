package edu.dair.sgdb.simulator;

import edu.dair.sgdb.simulator.tools.Edge;
import edu.dair.sgdb.simulator.tools.GraphGen;
import edu.dair.sgdb.simulator.graphloader.DirectedGraph;
import edu.dair.sgdb.simulator.graphloader.UndirectedGraph;
import edu.dair.sgdb.simulator.partalgs.EdgeCutHashing;
import edu.dair.sgdb.simulator.partalgs.Fennel;
import edu.dair.sgdb.simulator.partalgs.ThreePhase;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class SNAPMain {

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("./SNAPMain dir file_name type operation initial_threshold");
            return;
        }

        String dir = args[0];
        String file_name = args[1];
        String type = args[2];
        String op = args[3];
        int start_thres = Integer.parseInt(args[4]);

        String original_graph_file = dir + "/" + file_name;
        String metis_graph_file = original_graph_file + ".metis";
        String metis_graph_32partition_file = metis_graph_file + ".part.32";

        ArrayList<Edge> generated = null;

        if ("undirected".equalsIgnoreCase(type)) {
            generated = UndirectedGraph.load(original_graph_file);

        } else if ("directed".equalsIgnoreCase(type)) {
            generated = DirectedGraph.load(original_graph_file);
        } else {
            System.out.println("Wrong graph type, please specify: undirected or directed");
            return;
        }

        TIntObjectHashMap<TIntHashSet> csr = new TIntObjectHashMap<>();
        TIntObjectHashMap<TIntHashSet> reverse_csr = new TIntObjectHashMap<>();
        int max_vid = 0;
        //TreeMap<Integer, HashSet<Integer>> csr = new TreeMap<>();
        //TreeMap<Integer, HashSet<Integer>> reverse_csr = new TreeMap<>();
        int numVertices = 0;
        int real_edge_num = 0;

        for (Edge e : generated) {
            if (e.src == e.dst) continue;
            if (Math.max(e.src, e.dst) > max_vid)
                max_vid = Math.max(e.src, e.dst);

            real_edge_num++;

            if (!csr.containsKey(e.src)) csr.put(e.src, new TIntHashSet());
            if (!csr.containsKey(e.dst)) csr.put(e.dst, new TIntHashSet());
            csr.get(e.src).add(e.dst);

            if (!reverse_csr.containsKey(e.src)) reverse_csr.put(e.src, new TIntHashSet());
            if (!reverse_csr.containsKey(e.dst)) reverse_csr.put(e.dst, new TIntHashSet());
            reverse_csr.get(e.dst).add(e.src);
        }

        System.out.println("csr key size: " + csr.keys().length);

        StringBuilder outputs[] = new StringBuilder[max_vid + 1];

        /*
        TIntObjectIterator iterator = csr.iterator();
        while (iterator.hasNext()){
            int i = iterator.key();
            if (outputs[i] == null)
                outputs[i] = new StringBuilder();
            TIntHashSet set = (TIntHashSet) iterator.value();

            TIntIterator iter = set.iterator();
            while (iter.hasNext()){
                int j = iter.next();
                outputs[i].append(String.valueOf(j)).append(" ");
            }
        }
        */
        for (int i = 0; i <= max_vid; i++) {
            if (csr.containsKey(i)) {
                outputs[i] = new StringBuilder();
                TIntHashSet set = csr.get(i);
                TIntIterator iter = set.iterator();
                while (iter.hasNext()) {
                    int j = iter.next();
                    outputs[i].append(String.valueOf(j + 1)).append(" ");
                }
            }
            if (reverse_csr.containsKey(i)) {
                if (outputs[i] == null) outputs[i] = new StringBuilder();
                TIntHashSet set = reverse_csr.get(i);
                TIntIterator iter = set.iterator();
                while (iter.hasNext()) {
                    int j = iter.next();
                    outputs[i].append(String.valueOf(j + 1)).append(" ");
                }
            }
        }

        numVertices = max_vid + 1;

        if ("gengraph".equalsIgnoreCase(op)) {

            int vertices = 10000;
            int edges = 1200000;
            double pA = 0.45;
            double pB = 0.15;
            double pC = 0.15;
            double pD = 0.25;

            GraphGen generator = new GraphGen(pA, pB, pC, pD, vertices, edges);
            generator.execute();

            BufferedWriter bw = new BufferedWriter(new FileWriter(original_graph_file));
            for (Edge e : generator.generated) {
                String edge = e.src + " " + e.dst + "\n";
                bw.write(edge);
            }
            bw.close();
        }

        if ("metis".equalsIgnoreCase(op)) {
            BufferedWriter bw_metis = new BufferedWriter(new FileWriter(metis_graph_file));
            bw_metis.write(numVertices + " " + real_edge_num + "\n");

            for (int i = 0; i <= max_vid; i++) {
                if (outputs[i] != null) {
                    bw_metis.write(outputs[i].toString().trim() + "\n");
                } else {
                    bw_metis.write("\n");
                }
            }
            /*
            int last_key = -1;
            for (int key : csr.keySet()) {

                while (key > (last_key + 1)){
                    bw_metis.write("\n");
                    last_key += 1;
                }

                String line = "";
                Set<Integer> neighbors = new HashSet<>();
                neighbors.addAll(csr.get(key));
                neighbors.addAll(reverse_csr.get(key));
                for (int v : neighbors)
                    line += ((v + 1) + " ");
                bw_metis.write(line + "\n");
                last_key += 1;
            }
            */
            bw_metis.close();
        }

        if ("metis-count".equalsIgnoreCase(op)) {
            String metis_results = metis_graph_32partition_file;
            BufferedReader br_metis = new BufferedReader(new FileReader(metis_results));
            String line;
            int i = 0;

            HashMap<Integer, Integer> location = new HashMap<>();
            int counts[] = new int[32];
            for (int j = 0; j < 32; j++) counts[j] = 0;

            while ((line = br_metis.readLine()) != null) {
                location.put(i, Integer.valueOf(line));
                counts[Integer.valueOf(line)] += 1;
                i++;
            }

            for (i = 0; i < 32; i++)
                System.out.print(counts[i] + " ");
            System.out.println();

            HashSet<Edge> set = new HashSet<>();
            set.addAll(generated);
            int numEdges = set.size();

            int total_cut = 0;
            for (Edge e : set) {
                int src = e.src;
                int dst = e.dst;

                int src_loc = location.get(src);
                int dst_loc = location.get(dst);

                if (src_loc != dst_loc) {
                    total_cut += 1;
                }
            }
            System.out.println("discl.ttu.edu.MetisMain Total Cuts: " + total_cut + " Percent: " + (float) total_cut / (float) numEdges);
        }

        if ("fennel".equalsIgnoreCase(op)) {
            HashSet<Edge> set = new HashSet<>();
            set.addAll(generated);

            Fennel f = new Fennel(csr, reverse_csr, set, 32);
            f.workload_run_s();
        }

        if ("hash".equalsIgnoreCase(op)) {
            HashSet<Edge> set = new HashSet<>();
            set.addAll(generated);
            EdgeCutHashing.workload_run(set, 32);
        }

        if ("iogp".equalsIgnoreCase(op)) {
            HashSet<Edge> set = new HashSet<>();
            set.addAll(generated);
            HashSet<Edge> reversed = new HashSet<>();
            for (Edge e : set) {
                Edge ne = new Edge(e.dst, e.src);
                reversed.add(ne);
            }
            set.addAll(reversed);

            ThreePhase.workload_run(set, 32);
        }

        if ("iogpperf".equalsIgnoreCase(op)) {
            HashSet<Edge> set = new HashSet<>();
            set.addAll(generated);
            HashSet<Edge> reversed = new HashSet<>();
            for (Edge e : set) {
                Edge ne = new Edge(e.dst, e.src);
                reversed.add(ne);
            }
            set.addAll(reversed);

            for (int thrs = 1; thrs < 52; thrs += 5) {
                //System.out.println("-----------------------------------------");

                int reassinged = 0;
                TIntObjectIterator<TIntHashSet> iterator = csr.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    int key = iterator.key();
                    TIntHashSet value = iterator.value();
                    double k = (double) value.size() / (double) thrs;
                    reassinged += Math.max((int) (Math.log(k) / Math.log(2)), 0);
                }
                System.out.println("threshold: " + thrs + " reassign: " + reassinged + " :");

                ThreePhase.workload_run_threshold(set, thrs);
                //System.out.println("\n\n\n\n");
            }
        }

        if ("tmp".equalsIgnoreCase(op)) {
            for (int threshold = 1; threshold <= 51; threshold += 5) {
                int reassinged = 0;
                TIntObjectIterator<TIntHashSet> iterator = csr.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    int key = iterator.key();
                    TIntHashSet value = iterator.value();
                    double k = (double) value.size() / (double) threshold;
                    reassinged += Math.max((int) (Math.log(k) / Math.log(2)), 0);
                }
                System.out.println("threshold: " + threshold + " reassign: " + reassinged);
            }
        }

        if ("dist".equalsIgnoreCase(op)) {
            for (int threshold = 1; threshold <= 51; threshold += 5) {

                int vertex_number = 0;
                TIntObjectIterator<TIntHashSet> iterator = csr.iterator();
                while (iterator.hasNext()) {
                    iterator.advance();
                    int key = iterator.key();
                    TIntHashSet value = iterator.value();
                    if (value.size() >= threshold)
                        vertex_number += 1;
                }

                System.out.println("threshold: " + threshold + " over: " + vertex_number);
            }
        }

    }
}
