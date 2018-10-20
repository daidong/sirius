package edu.dair.sgdb.gserver;

import edu.dair.sgdb.gserver.dido.DIDOSrv;
import edu.dair.sgdb.gserver.edgecut.EdgeCutSrv;
import edu.dair.sgdb.gserver.giga.GIGASrv;
import edu.dair.sgdb.gserver.iogp.IOGPSrv;
import edu.dair.sgdb.gserver.vertexcut.VertexCutSrv;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangwei on 8/12/15.
 * Modified by Dong Dai as there is a bug in distributed mode
 */
public class ServerExecutable implements Runnable {

    private String type;
    private String db;
    private int localIdx = -1;
    private List<String> allServers = new ArrayList<String>() {
        {
            //add("");
        }
    };

    public ServerExecutable(String type, String db, int localIdx, List<String> allServers) {
        this.type = type;
        this.db = db;
        this.localIdx = localIdx;
        this.allServers = allServers;
    }

    private void initialCheck() {
        if (type == null || db == null || localIdx < 0 || allServers.isEmpty()) {
            System.err.println("type, dbnum, idx or allServers are not well-configured. Please check the parameter.");
            System.exit(1);
        }
    }

    @Override
    public void run() {
        initialCheck();
        AbstractSrv abstractSrv = null;
        switch (type) {
            case "edgecut":
                abstractSrv = new EdgeCutSrv();
                break;
            case "iogp":
                abstractSrv = new IOGPSrv();
                break;
            case "giga":
                abstractSrv = new GIGASrv();
                break;
            case "dido":
                abstractSrv = new DIDOSrv();
                break;
            case "vertexcut":
                abstractSrv = new VertexCutSrv();
                break;
            default:
                System.out.println("Undefined Server Type!");
                break;
        }
        abstractSrv.setDbFile(this.getDb());
        abstractSrv.setLocalIdx(this.getLocalIdx());
        abstractSrv.setAllSrvs(this.getAllServers());
        abstractSrv.runit();
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public int getLocalIdx() {
        return localIdx;
    }

    public void setLocalIdx(int localIdx) {
        this.localIdx = localIdx;
    }

    public List<String> getAllServers() {
        return allServers;
    }

    public void setAllServers(List<String> allServers) {
        this.allServers = allServers;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
