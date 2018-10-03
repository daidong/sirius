package edu.dair.sgdb.simulator.partalgs;

import edu.dair.sgdb.simulator.tools.Edge;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by daidong on 5/30/16.
 */
public class SplitOnly {

    public static final int MAX_EDGES = 200;

    public HashMap<Integer, Integer> degree = new HashMap<>();
    public HashMap<Integer, Boolean> split = new HashMap<>();

    public ArrayList<Integer> v = new ArrayList<>();
    public ArrayList<Edge> e = new ArrayList<>();

    public ArrayList<SplitOnly> cluster;

    public int serverNumber;
    public int index;

    public SplitOnly(int index, ArrayList<SplitOnly> cluster, int num) {
        this.cluster = cluster;
        this.index = index;
        this.serverNumber = num;
    }

    public int hash(int vid) {
        return vid % serverNumber;
    }

    public void insertV(int vid) {
        this.v.add(vid);
        this.degree.put(vid, 0);
        this.split.put(vid, Boolean.FALSE);
    }

    public int insertE(int src, int dst) {
        boolean enterSplit = false;

        Edge newEdge = new Edge(src, dst);
        this.e.add(newEdge);

        if (!degree.containsKey(src)) degree.put(src, 0);
        degree.put(src, degree.get(src) + 1);

        // post insertion
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
                SplitOnly target = cluster.get(hash(edge.dst));
                target.insertE(edge.src, edge.dst);
                this.e.remove(edge);
            }

            enterSplit = true;
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
