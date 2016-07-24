package edu.ttu.discl.iogp.gserver;

public enum EdgeType {

    STATIC_ATTR(0),
    DYNAMIC_ATTR(1),
    RUN(2),
    WAS_RUN_BY(3),
    CONTAIN(4),
    BELONG_TO(5),
    EXECUTE(6),
    EXECUTE_BY(7),
    JOB_HAS(8),
    BELONG_TO_JOB(9);

    private final int value;

    EdgeType(int value) {
        this.value = value;
    }

    public int get() {
        return value;
    }

    public static EdgeType getEnumFromValue(int v) {
        for (EdgeType e : EdgeType.values()) {
            if (e.value == v) {
                return e;
            }
        }
        return null;
    }

    public static EdgeType getEnumFromName(String name) {
        for (EdgeType e : EdgeType.values()) {
            if (e.name().equalsIgnoreCase(name)) {
                return e;
            }
        }
        return null;
    }

}
