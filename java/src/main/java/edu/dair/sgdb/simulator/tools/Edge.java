package edu.dair.sgdb.simulator.tools;

/**
 * Created by daidong on 5/30/16.
 */
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