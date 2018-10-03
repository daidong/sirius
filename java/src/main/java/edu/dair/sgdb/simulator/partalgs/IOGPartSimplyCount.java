package edu.dair.sgdb.simulator.partalgs;

import edu.dair.sgdb.simulator.tools.Edge;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by daidong on 5/30/16.
 */
public class IOGPartSimplyCount {

    public static final int MAX_EDGES = 200;
    public static final int MAX_REASSIGN = 20;

    public HashMap<Integer, Integer> degree = new HashMap<>();
    public HashMap<Integer, Integer> loc = new HashMap<>();
    public HashMap<Integer, Boolean> split = new HashMap<>();
    public HashMap<Integer, Integer> ra = new HashMap<>();

    public ArrayList<Integer> v = new ArrayList<>();
    public ArrayList<Edge> e = new ArrayList<>();

    public ArrayList<IOGPartSimplyCount> cluster;

    public int serverNumber;
    public int index;

    public IOGPartSimplyCount(int index, ArrayList<IOGPartSimplyCount> cluster, int num) {
        this.cluster = cluster;
        this.index = index;
        this.serverNumber = num;
    }

    public int hash(int vid) {
        return vid % serverNumber;
    }

    public void insertV(int vid) {
        this.v.add(vid);
        this.ra.put(vid, 1);

        this.degree.put(vid, 0);

        this.loc.put(vid, index);
        this.split.put(vid, Boolean.FALSE);
    }

    public void reassignV(int vid) {
        this.v.add(vid);
        loc.put(vid, this.index);
    }

    public int insertE(int src, int dst) {
        boolean enterSplit = false;

        Edge newEdge = new Edge(src, dst);
        this.e.add(newEdge);

        if (!degree.containsKey(src)) degree.put(src, 0);
        degree.put(src, degree.get(src) + 1);

        // post insertion
        // do we need split?
        if (degree.get(src) > MAX_EDGES &&
                split.containsKey(src) &&
                split.get(src) == Boolean.FALSE) {

            split.put(src, Boolean.TRUE);

            ArrayList<Edge> rms = new ArrayList<>();
            for (Edge edge : e) {
                if (edge.src == src && hash(edge.dst) != index) {
                    rms.add(edge);
                }
            }

            degree.put(src, degree.get(src) - rms.size());

            for (Edge edge : rms) {
                IOGPartSimplyCount target = cluster.get(hash(edge.dst));
                target.insertE(edge.src, edge.dst);
                this.e.remove(edge);
            }

            enterSplit = true;
        }

        // Only the server that originally stores src will do the reassignment.
        if (split.containsKey(src) &&
                split.get(src) == Boolean.TRUE &&
                degree.get(src) > (MAX_REASSIGN * ra.get(src))) {

            int total_size = this.e.size() + this.v.size();

            ra.put(src, ra.get(src) * 2);

            int from = this.loc.get(src);
            int max_server = from;
            int fennel_score = 0;

            for (Edge edge : cluster.get(from).e) {
                if (edge.src == src) {
                    fennel_score += 1;
                }

                if (edge.dst == src) {
                    fennel_score += 1;
                }
            }

            for (IOGPartSimplyCount part : cluster)
                if (part.index != from) {
                    int local_fennel = 0;
                    for (Edge edge : part.e) {
                        if (edge.src == src) local_fennel += 1;
                        if (edge.dst == src) local_fennel += 1;
                    }
                    if (local_fennel > fennel_score) {
                        System.out.println("Difference is: " + (local_fennel - fennel_score));
                        max_server = part.index;
                        fennel_score = local_fennel;
                    }
                }

            if (max_server != from) {

                cluster.get(from).v.remove((Integer) src);

                if (from != index) cluster.get(from).loc.remove(src);

                loc.put(src, max_server);
                cluster.get(max_server).reassignV(src);
                System.out.println("Reassign Vertex: " + src + " from " + from + " to " + max_server);
            }
        }

        if (enterSplit) return 1;
        else return 0;
    }


    @Override
    public String toString() {
        String str = "V{" + this.v.size() + "}:[";

        for (int vertex : this.v)
            str += (vertex + ", ");

        str += "];\nE{" + this.e.size() + "}:[";

        for (Edge edge : this.e) {
            str += ("(" + edge.src + "->" + edge.dst + "), ");
        }
        str += "];\n";
        return str;
    }
}
