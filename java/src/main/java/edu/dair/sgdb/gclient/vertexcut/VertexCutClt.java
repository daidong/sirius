package edu.dair.sgdb.gclient.vertexcut;

import edu.dair.sgdb.gclient.GraphClt;
import edu.dair.sgdb.gserver.EdgeType;
import edu.dair.sgdb.thrift.KeyValue;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class VertexCutClt extends GraphClt {

    private static final Logger logger = LoggerFactory.getLogger(VertexCutClt.class);

    public VertexCutClt(int port, ArrayList<String> alls) {
        super(port, alls);
    }

    @Override
    public List<KeyValue> read(byte[] srcVertex, EdgeType edgeType, byte[] dstKey) throws TException {
        return null;
    }

    @Override
    public int insert(byte[] srcVertex, EdgeType edgeType, byte[] dstKey, byte[] value) throws TException {
        return 0;
    }

    @Override
    public List<KeyValue> scan(byte[] srcVertex, EdgeType edgeType) throws TException {
        return null;
    }

    @Override
    public List<ByteBuffer> bfs(byte[] srcVertex, EdgeType edgeType, int max_steps) throws TException {
        return null;
    }

    @Override
    public int syncstatus() throws TException {
        return 0;
    }
}
