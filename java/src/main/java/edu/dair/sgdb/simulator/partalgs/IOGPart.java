package edu.dair.sgdb.simulator.partalgs;

import edu.dair.sgdb.simulator.tools.Edge;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by daidong on 5/30/16.
 */
public class IOGPart {

    public static final int MAX_EDGES = 500;
    public static final int MAX_REASSIGN = 50;
    public static final int SPACE_FACTOR_ALPHA = 5;
    public static final int SPACE_FACTOR_GAMMA = 5;

    public HashMap<Integer, Integer> plo = new HashMap<>();
    public HashMap<Integer, Integer> alo = new HashMap<>();
    public HashMap<Integer, Integer> pli = new HashMap<>();
    public HashMap<Integer, Integer> ali = new HashMap<>();
    public HashMap<Integer, Integer> degree = new HashMap<>();


    public HashMap<Integer, Integer> loc = new HashMap<>();
    public HashMap<Integer, Boolean> split = new HashMap<>();
    public HashMap<Integer, Integer> ra = new HashMap<>();

    public ArrayList<Integer> v = new ArrayList<>();
    public ArrayList<Edge> e = new ArrayList<>();

    public ArrayList<IOGPart> cluster;

    public int serverNumber;
    public int index;

    public IOGPart(int index, ArrayList<IOGPart> cluster, int num) {
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

        this.plo.put(vid, 0);
        this.alo.put(vid, 0);
        this.pli.put(vid, 0);
        this.ali.put(vid, 0);
        this.degree.put(vid, 0);

        this.loc.put(vid, index);
        this.split.put(vid, Boolean.FALSE);
    }

    public void reassignV(int vid) {
        this.v.add(vid);

        alo.put(vid, plo.get(vid));
        plo.put(vid, 0);

        ali.put(vid, pli.get(vid));
        pli.put(vid, 0);

        loc.put(vid, this.index);
    }

    public int insertE(int src, int dst) {
        boolean enterSplit = false;

        Edge newEdge = new Edge(src, dst);
        this.e.add(newEdge);

        if (!plo.containsKey(src)) plo.put(src, 0);
        if (!alo.containsKey(src)) alo.put(src, 0);
        if (!pli.containsKey(dst)) pli.put(dst, 0);
        if (!ali.containsKey(dst)) ali.put(dst, 0);
        if (!degree.containsKey(src)) degree.put(src, 0);

        degree.put(src, degree.get(src) + 1);

        // this vertex has never split yet
        if (split.containsKey(src) && split.get(src) == Boolean.FALSE) {

            // edge and dst are together
            if (loc.containsKey(dst) && loc.get(dst) == index) {
                alo.put(src, alo.get(src) + 1);
                ali.put(dst, ali.get(dst) + 1);
            } else {
                pli.put(dst, pli.get(dst) + 1);
                alo.put(src, alo.get(src) + 1);
            }
        }
        // this vertex was split before => Split Phase
        else {
            // the edge is stored to hash(dst)%m all the times
            if (hash(dst) != index)
                System.out.println("Error Assinging Edges of Split Vertices");

            // source and edge are together
            if (loc.containsKey(src) && loc.get(src) == index) {
                if (loc.containsKey(dst) && loc.get(dst) == index) {
                    //source -> edge -> dst are in the same server
                    alo.put(src, alo.get(src) + 1);
                    ali.put(dst, ali.get(dst) + 1);
                } else {
                    //source -> edge are in the same server, dst is not
                    pli.put(dst, pli.get(dst) + 1);
                    alo.put(src, alo.get(src) + 1);
                }
            }
            // source and edge are not together
            else {
                if (loc.containsKey(dst) && loc.get(dst) == index) {
                    //source \- edge -> dst
                    plo.put(src, plo.get(src) + 1);
                    ali.put(dst, ali.get(dst) + 1);
                } else {
                    //src \- edge \- dst
                    plo.put(src, plo.get(src) + 1);
                    pli.put(dst, pli.get(dst) + 1);
                }
            }
        }


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

            alo.put(src, alo.get(src) - rms.size());
            degree.put(src, degree.get(src) - rms.size());

            for (Edge edge : rms) {
                pli.put(edge.dst, pli.get(edge.dst) - 1);

                IOGPart target = cluster.get(hash(edge.dst));
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
            int fennel_score = cluster.get(from).ali.get(src) +
                    cluster.get(from).alo.get(src);

            for (IOGPart part : cluster)
                if (part.index != from &&
                        part.pli.containsKey(src) &&
                        part.plo.containsKey(src) &&
                        part.pli.get(src) + part.plo.get(src) > fennel_score) {

                    max_server = part.index;
                    System.out.println("Difference is: " + (part.pli.get(src) + part.plo.get(src) - fennel_score));
                    fennel_score = part.pli.get(src) + part.plo.get(src);
                }

            if (max_server != from) {

                cluster.get(from).v.remove((Integer) src);
                cluster.get(from).plo.put(src, alo.get(src));
                cluster.get(from).alo.put(src, 0);
                cluster.get(from).pli.put(src, ali.get(src));
                cluster.get(from).ali.put(src, 0);

                if (from != index)
                    // we have reassign this vertex before
                    cluster.get(from).loc.remove(src);

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
