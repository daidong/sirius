package edu.dair.sgdb.gclient;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class GigaClt {
    public HashMap<ByteBuffer, GigaIndex> gigaMaps;

    public GigaClt(int port, ArrayList<String> alls) {
        super(port, alls);
        this.gigaMaps = new HashMap<>();
    }
}
