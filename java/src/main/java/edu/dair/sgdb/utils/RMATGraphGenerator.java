package edu.dair.sgdb.utils;

import java.io.*;
import java.util.*;

/**
 * Graph generator based on the R-MAT algorithm
 * R-MAT: A Recursive Model for Graph Mining
 * Chakrabarti, Zhan, Faloutsos: http://www.cs.cmu.edu/~christos/PUBLICATIONS/siam04.pdf
 */
public class RMATGraphGenerator {

    /* Parameters for top-left, top-right, bottom-left, bottom-right probabilities */
    private double pA, pB, pC, pD;
    private long numEdges;
    private int numVertices;

    public class Edge {
        public int src;
        public int dst;

        public Edge(int s, int d) {
            this.src = s;
            this.dst = d;
        }

        @Override
        public String toString() {
            return "[" + src + ", " + dst + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 7;
            result = prime * result;
            result = prime * result + dst;
            result = prime * result + src;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Edge other = (Edge) obj;
            if (dst != other.dst)
                return false;
            return src == other.src;
        }

    }

    public ArrayList<Edge> generated;

    /**
     * From http://pywebgraph.sourceforge.net
     * ## Probability of choosing quadrant A
     * self.probA = 0.45
     * <p>
     * ## Probability of choosing quadrant B
     * self.probB = 0.15
     * <p>
     * ## Probability of choosing quadrant C
     * self.probC = 0.15
     * <p>
     * ## Probability of choosing quadrant D
     * self.probD = 0.25
     */


    public RMATGraphGenerator(double pA, double pB, double pC, double pD, int nVertices, long nEdges) {
        this.pA = pA;
        this.pB = pB;
        this.pC = pC;
        this.pD = pD;
        this.generated = new ArrayList<Edge>();

        if (Math.abs(pA + pB + pC + pD - 1.0) > 0.01)
            throw new IllegalArgumentException("Probabilities do not add up to one!");
        numVertices = nVertices;
        numEdges = nEdges;
    }

    public void execute() {

        int nEdgesATime = 1000000;
        long createdEdges = 0;

        Random r = new Random(System.currentTimeMillis() + this.hashCode());

        double cumA = pA;
        double cumB = cumA + pB;
        double cumC = cumB + pC;
        double cumD = 1.0;
        assert (cumD > cumC);

        while (numEdges > createdEdges) {
            int ne = (int) Math.min(numEdges - createdEdges, nEdgesATime);
            int[] fromIds = new int[ne];
            int[] toIds = new int[ne];

            for (int j = 0; j < ne; j++) {
                int col_st = 0, col_en = numVertices - 1, row_st = 0, row_en = numVertices - 1;
                while (col_st != col_en || row_st != row_en) {
                    double x = r.nextDouble();

                    if (x < cumA) {
                        // Top-left
                        col_en = col_st + (col_en - col_st) / 2;
                        row_en = row_st + (row_en - row_st) / 2;
                    } else if (x < cumB) {
                        // Top-right
                        col_st = col_en - (col_en - col_st) / 2;
                        row_en = row_st + (row_en - row_st) / 2;

                    } else if (x < cumC) {
                        // Bottom-left
                        col_en = col_st + (col_en - col_st) / 2;
                        row_st = row_en - (row_en - row_st) / 2;
                    } else {
                        // Bottom-right
                        col_st = col_en - (col_en - col_st) / 2;
                        row_st = row_en - (row_en - row_st) / 2;
                    }
                }
                fromIds[j] = col_st;
                toIds[j] = row_st;
            }

            this.addEdges(fromIds, toIds);
            createdEdges += ne;
            //System.out.println(Thread.currentThread().getId() + " created " + createdEdges + " edges.");
        }
    }

    public void addEdges(int[] src, int[] dst) {
        int len = src.length;
        for (int i = 0; i < len; i++) {
            this.generated.add(new Edge(src[i], dst[i]));
        }
    }

    public static void main(String[] args) throws IOException {
        int k = 0;
        int numVertices = Integer.parseInt(args[k++]);
        long numEdges = Long.parseLong(args[k++]);
        String file = args[k++];
        String sum = args[k++];

		/*
        double pA = Double.parseDouble(args[k++]);
        double pB = Double.parseDouble(args[k++]);
        double pC = Double.parseDouble(args[k++]);
        double pD = Double.parseDouble(args[k++]);
		*/
        long t = System.currentTimeMillis();

        double pA = 0.45, pB = 0.15, pC = 0.15, pD = 0.25;

        HashMap<Integer, Integer> degreeMap = new HashMap<Integer, Integer>();
        TreeMap<Integer, TreeSet<Integer>> reversedMap = new TreeMap<Integer, TreeSet<Integer>>();

        RMATGraphGenerator generator = new RMATGraphGenerator(pA, pB, pC, pD, numVertices, numEdges);
        generator.execute();

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(file))));
        BufferedWriter summary = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(sum))));

        for (Edge e : generator.generated) {
            String data = ("vertex" + e.src) + " " + ("vertex" + e.dst);
            bw.write(data);
            bw.newLine();

            if (!degreeMap.containsKey(e.src)) {
                degreeMap.put(e.src, 0);
            }
            int value = degreeMap.get(e.src);
            degreeMap.put(e.src, value + 1);
        }
        bw.close();

        for (int id : degreeMap.keySet()) {
            if (!reversedMap.containsKey(degreeMap.get(id))) {
                reversedMap.put(degreeMap.get(id), new TreeSet<Integer>());
            }
            reversedMap.get(degreeMap.get(id)).add(id);
        }

        for (int degree : reversedMap.keySet()) {
            summary.write(String.valueOf(degree));
            summary.write(" ");
            for (int id : reversedMap.get(degree)) {
                summary.write(String.valueOf(id));
                summary.write(" ");
            }
            summary.newLine();
        }
        summary.close();

        System.out.println("Generating took " + (System.currentTimeMillis() - t) * 0.001 + " secs");

    }

}