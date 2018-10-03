package edu.dair.sgdb.tengine.travel;

import java.util.Comparator;

public class TripleComparator implements Comparator<Triple> {
    @Override
    public int compare(Triple o1, Triple o2) {
        if (o1.tid < o2.tid)
            return -1;
        if (o1.tid > o2.tid)
            return 1;
        if (o1.sid < o2.sid)
            return -1;
        if (o1.sid > o2.sid)
            return 1;
        String v1 = String.format("%32s", new String(o1.vid));
        String v2 = String.format("%32s", new String(o2.vid));
        return v1.compareTo(v2);
    }

}