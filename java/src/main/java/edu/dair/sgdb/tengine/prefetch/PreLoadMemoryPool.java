package edu.dair.sgdb.tengine.prefetch;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by daidong on 12/14/15.
 */
public class PreLoadMemoryPool {

    public HashMap<ByteBuffer, HashSet<ByteBuffer>> EdgesPool;
    public int cachedEdgeItems = 0;
    public HashSet<ByteBuffer> VerticePool;
    public int cachedVertexItem = 0;

    public PreLoadMemoryPool() {
        this.EdgesPool = new HashMap<>();
        this.VerticePool = new HashSet<>();

    }

    public void addVertices(Set<ByteBuffer> vs) {
        synchronized (VerticePool) {
            this.VerticePool.addAll(vs);
            cachedVertexItem += 1;
        }
    }

    public void addEdge(ByteBuffer src, ByteBuffer dst) {
        synchronized (EdgesPool) {
            if (!EdgesPool.containsKey(src))
                EdgesPool.put(src, new HashSet<ByteBuffer>());
            HashSet<ByteBuffer> t = EdgesPool.get(src);
            t.add(dst);
            cachedEdgeItems += 1;
        }
    }

    public HashSet<ByteBuffer> getEdges(ByteBuffer src) {
        synchronized (EdgesPool) {
            if (EdgesPool.containsKey(src))
                return EdgesPool.get(src);
            else
                return null;
        }
    }

    public boolean has(ByteBuffer src) {
        synchronized (EdgesPool) {
            if (EdgesPool.containsKey(src))
                return true;
            return false;
        }
    }

    public int getCachedItemNumber() {
        return (cachedVertexItem + cachedEdgeItems);
    }

}
