package edu.dair.sgdb.gserver;

public enum EdgeType {

    STATIC_ATTR(0),
    DYNAMIC_ATTR(1),

    IN(2),
    OUT(3);

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
