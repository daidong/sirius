package edu.dair.sgdb.tengine.travel;

import java.util.ArrayList;
import java.util.List;

public class GTravel {

    private List<SingleStep> steps;
    private int step;

    public GTravel() {
        this.steps = new ArrayList<SingleStep>();
        step = 0;
        this.steps.add(step, new SingleStep());
    }

    public GTravel v() {
        return this;
    }

    public GTravel v(byte[] key) {
        SingleStep ss = this.steps.get(step);
        ss.vertexKeyRestrict = new SingleRestriction.EQ("key".getBytes(), key);
        return this;
    }

    public GTravel v(List<byte[]> keySet) {
        SingleStep ss = this.steps.get(step);
        ss.vertexKeyRestrict = new SingleRestriction.InWithValues("key".getBytes(), keySet);
        return this;
    }

    public GTravel va(byte[] key, byte[] value) {
        SingleStep ss = this.steps.get(step);
        ss.vertexDynAttrRestrict = new SingleRestriction.EQ(key, value);
        return this;
    }

    public GTravel va(byte[] key, List<byte[]> values) {
        SingleStep ss = this.steps.get(step);
        ss.vertexDynAttrRestrict = new SingleRestriction.InWithValues(key, values);
        return this;
    }

    public GTravel va(byte[] key, byte[] start, byte[] end, boolean start_inclusive, boolean end_inclusive) {
        SingleStep ss = this.steps.get(step);
        ss.vertexDynAttrRestrict = new SingleRestriction.Slice(key, start, end, start_inclusive, end_inclusive);
        return this;
    }

    public GTravel et(byte[] type) {
        SingleStep ss = this.steps.get(step);
        ss.typeRestrict = new SingleRestriction.EQ("type".getBytes(), type);
        return this;
    }

    public GTravel et(List<byte[]> types) {
        SingleStep ss = this.steps.get(step);
        ss.typeRestrict = new SingleRestriction.InWithValues("type".getBytes(), types);
        return this;
    }

    public GTravel ea(byte[] key, byte[] value) {
        SingleStep ss = this.steps.get(step);
        ss.edgeKeyRestrict = new SingleRestriction.EQ(key, value);
        return this;
    }

    public GTravel ea(byte[] key, byte[] start, byte[] end) {
        SingleStep ss = this.steps.get(step);
        ss.edgeKeyRestrict = new SingleRestriction.Range(key, start, end);
        return this;
    }

    public GTravel next() {
        this.step++;
        this.steps.add(this.step, new SingleStep());
        return this;
    }

    public List<SingleStep> plan() {
        return this.steps;
    }

}
