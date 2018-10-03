package edu.dair.sgdb.tengine.travel;

public enum Bound {
    START(0), END(1);

    public final int idx;

    Bound(int idx) {
        this.idx = idx;
    }

    public static Bound reverse(Bound b) {
        return b == START ? END : START;
    }
}