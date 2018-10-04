package edu.dair.sgdb.gclient;

import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.sengine.DBKey;
import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.NIOHelper;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class VertexCutClt extends AbstractClt {

    private static final Logger logger = LoggerFactory.getLogger(VertexCutClt.class);

    public VertexCutClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    @Override
    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        int dstServer = getHashLocation(dstKey, this.serverNum);
        return getClientConn(dstServer).read(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get());
    }

    @Override
    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        int dstServer = getHashLocation(dstKey, this.serverNum);
        return getClientConn(dstServer).insert(ByteBuffer.wrap(srcVertex), ByteBuffer.wrap(dstKey), edgeType.get(), ByteBuffer.wrap(value));
    }

    @Override
    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException {
        int dstServer = getHashLocation(srcVertex, this.serverNum);
        return getClientConn(dstServer).scan(ByteBuffer.wrap(srcVertex), edgeType.get());
    }

    private class BfsEntry {
        public ByteBuffer v;
        public int step;

        public BfsEntry(ByteBuffer b, int s) {
            v = b;
            step = s;
        }
    }

    @Override
    public List<ByteBuffer> bfs(byte[] srcVertex, EdgeType edgeType, int max_steps) throws TException {
        HashSet<ByteBuffer> visited = new HashSet<>();
        LinkedBlockingQueue<BfsEntry> queue = new LinkedBlockingQueue<>();

        queue.offer(new BfsEntry(ByteBuffer.wrap(srcVertex), 0));

        BfsEntry current;

        while ((current = queue.poll()) != null) {
            byte[] key = NIOHelper.getActiveArray(current.v);
            int step = current.step;

            visited.add(current.v);

            if (step < max_steps) {

                List<KeyValue> results = scan(key, edgeType);

                for (KeyValue kv : results) {
                    DBKey newKey = new DBKey(kv.getKey());
                    byte[] dst = newKey.dst;
                    if (!visited.contains(ByteBuffer.wrap(dst)))
                        queue.offer(new BfsEntry(ByteBuffer.wrap(dst), step + 1));
                }
            }
        }
        return new ArrayList<>(visited);
    }

    @Override
    public int sync() throws TException {
        return 0;
    }
}
