package edu.ttu.discl.iogp.gclient;

import edu.ttu.discl.iogp.gserver.EdgeType;
import edu.ttu.discl.iogp.sengine.DBKey;
import edu.ttu.discl.iogp.thrift.KeyValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Batch {

    private HashMap<ByteBuffer, ArrayList<KeyValue>> kvs;

    public Batch() {
        this.kvs = new HashMap<>();
    }

    public void append(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value, long ts) {
        ByteBuffer comparableSrc = ByteBuffer.wrap(srcVertex);
        DBKey dbKey = new DBKey(srcVertex, dstKey, edgeType.get(), ts);

        KeyValue kv = new KeyValue();
        kv.setKey(dbKey.toKey());
        kv.setValue(value);
        if (!kvs.containsKey(comparableSrc))
            kvs.put(comparableSrc, new ArrayList<KeyValue>());
        kvs.get(comparableSrc).add(kv);
    }

    public Set<ByteBuffer> keySet() {
        return this.kvs.keySet();
    }

    public List<KeyValue> getEntries(ByteBuffer src) {
        return this.kvs.get(src);
    }

}
