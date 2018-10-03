package edu.dair.sgdb.tengine.travel;

public class TravelDescriptor {

    long travelId;
    int stepId;
    int serverId;

    public TravelDescriptor(long tid, int sid, int server) {
        this.travelId = tid;
        this.stepId = sid;
        this.serverId = server;
    }

    @Override
    public String toString() {
        return "[" + travelId + ":" + serverId + ":" + stepId + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) travelId;
        result = prime * result + serverId;
        result = prime * result + stepId;
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
        TravelDescriptor other = (TravelDescriptor) obj;
        if (travelId != other.travelId) {
            return false;
        }
        if (serverId != other.serverId) {
            return false;
        }
        return stepId == other.stepId;
    }
}
