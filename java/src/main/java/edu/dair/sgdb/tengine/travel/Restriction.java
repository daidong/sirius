package edu.dair.sgdb.tengine.travel;

import org.json.simple.JSONObject;

import java.util.List;

/**
 * There are 'selector' and 'filter', two different restrictions.
 * We can check the attribute value of key to filter out the bad case;
 * we can also check the attribute value of value to filter out the bad case; But,
 * more importantly, we need to use "range" to selector stuff:
 * -> select the range of keys, types, timestamps, edge Ids
 * key | type | timestamps | edge
 * <p>
 * FilterRestriction: EQ, IN, SLICE; <key, value-set>
 * SelectRestriction: RANGE; <{"key", "type", "ts", "edge"}, [start, end)>
 */
public interface Restriction {

    boolean isSlice();

    boolean isEQ();

    boolean isIN();

    boolean isRange();

    byte[] key();

    List<byte[]> values();

    void setValues(List<byte[]> v);

    boolean satisfy(byte[] value);

    String genJSONString();

    JSONObject genJSON();

    String toString();

    interface EQ extends Restriction {
    }

    interface IN extends Restriction {
    }

    interface Slice extends Restriction {
    }

    interface Range extends Restriction {
        byte[] starter();

        byte[] end();
    }

}
