package edu.dair.sgdb.tengine.travel;

import java.util.List;

public class SingleRelation extends Relation {

    private byte[] entity;
    private byte[] value;
    private List<byte[]> inValues;

    public SingleRelation(byte[] entity, Type type, byte[] value, List<byte[]> inValues) {
        this.entity = entity;
        this.relationType = type;
        this.value = value;
        this.inValues = inValues;
    }

    public SingleRelation(byte[] entity, Type type, byte[] value) {
        this(entity, type, value, null);
    }

    public static SingleRelation createInRelation(byte[] entity, List<byte[]> inValues) {
        return new SingleRelation(entity, Type.IN, null, inValues);
    }

    public byte[] getEntity() {
        return this.entity;
    }

    public byte[] getValue() {
        assert relationType != Type.IN || value == null;
        return value;
    }

    public List<byte[]> getInValues() {
        assert relationType == Type.IN;
        return inValues;
    }

    @Override
    public String toString() {
        if (relationType == Type.IN)
            return String.format("%s IN %s", new String(entity), inValues);
        else
            return String.format("%s %s %s", new String(entity), relationType, value);
    }
}
