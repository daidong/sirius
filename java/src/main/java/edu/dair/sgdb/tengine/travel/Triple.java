package edu.dair.sgdb.tengine.travel;

import java.util.Arrays;

public class Triple {

    public long tid;
    public int sid;
    public byte[] vid;
    public byte type;

    public Triple(long tid, int sid, byte[] vid, byte type) {
        this.tid = tid;
        this.sid = sid;
        this.vid = vid;
        this.type = type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result;
        result = prime * result + sid;
        result = prime * result + (int) (tid ^ (tid >>> 32));
        result = prime * result + Arrays.hashCode(vid);
        result = prime * result + (int) type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Triple other = (Triple) obj;
        if (sid != other.sid) {
            return false;
        }
        if (tid != other.tid) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return Arrays.equals(vid, other.vid);
    }
}
