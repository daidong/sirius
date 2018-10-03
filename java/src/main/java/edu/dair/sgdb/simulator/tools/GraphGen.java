package edu.dair.sgdb.simulator.tools;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

public class GraphGen {
    /* Parameters for top-left, top-right, bottom-left, bottom-right probabilities */
    private double pA, pB, pC, pD;
    private long numEdges;
    private int numVertices;

    public HashSet<Edge> generated;

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

    public GraphGen(String file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            this.generated = new HashSet<>();
            String line;
            while ((line = br.readLine()) != null) {
                String[] l = line.split(" ");
                this.generated.add(new Edge(Integer.valueOf(l[0]), Integer.valueOf(l[1])));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public GraphGen(double pA, double pB, double pC, double pD, int nVertices, long nEdges) {
        this.pA = pA;
        this.pB = pB;
        this.pC = pC;
        this.pD = pD;
        this.generated = new HashSet<Edge>();

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
}
