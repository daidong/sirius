package edu.ttu.discl.iogp.gserver;

public enum EdgeType {

    IN(0),
    OUT(1),
    STATIC_ATTR(2),
    DYNAMIC_ATTR(3);

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
