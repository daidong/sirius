package edu.dair.sgdb.tengine.travel;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class SingleRestriction implements Restriction {

    public static SingleRestriction parseJSON(String json) {
        JSONCommand js = new JSONCommand();
        Map r = js.parse(json);
        String type = (String) r.get("type");
        if ("eq".equalsIgnoreCase(type)) {
            return SingleRestriction.EQ.parseJSON(json);
        } else if ("in".equalsIgnoreCase(type)) {
            return SingleRestriction.InWithValues.parseJSON(json);
        } else if ("slice".equalsIgnoreCase(type)) {
            return SingleRestriction.Slice.parseJSON(json);
        } else if ("range".equalsIgnoreCase(type)) {
            return SingleRestriction.Range.parseJSON(json);
        }
        return null;
    }

    public static class EQ extends SingleRestriction implements Restriction.EQ {
        protected byte[] key;
        protected byte[] value;

        public EQ(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public List<byte[]> values() {
            return Collections.singletonList(value);
        }

        public byte[] key() {
            return this.key;
        }

        public boolean isSlice() {
            return false;
        }

        public boolean isEQ() {
            return true;
        }

        public boolean isIN() {
            return false;
        }

        public boolean isRange() {
            return false;
        }

        @Override
        public String genJSONString() {
            JSONObject json = new JSONObject();
            json.put("type", "eq");
            json.put("key", new String(key));
            json.put("val", new String(value));
            return json.toString();
        }

        public static SingleRestriction.EQ parseJSONString(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);
            Relation.Type type = (Relation.Type) r.get("type");
            String value = (String) r.get("val");
            String key = (String) r.get("key");
            return new SingleRestriction.EQ(key.getBytes(), value.getBytes());
        }

        @Override
        public boolean satisfy(byte[] val) {
            String cond = new String(this.value);
            String v = new String(val);
            return v.equalsIgnoreCase(cond);
        }

        @Override
        public void setValues(List<byte[]> v) {
            this.value = v.get(0);
        }

        @Override
        public JSONObject genJSON() {
            JSONObject json = new JSONObject();
            json.put("type", "eq");
            json.put("key", new String(key));
            json.put("val", new String(value));
            return json;
        }

        public static SingleRestriction.EQ parseJSON(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);
            String value = (String) r.get("val");
            String key = (String) r.get("key");
            return new SingleRestriction.EQ(key.getBytes(), value.getBytes());
        }

        @Override
        public String toString() {
            String str = new String(key) + " eq " + new String(value);
            return str;
        }
    }

    public static class InWithValues extends SingleRestriction implements Restriction.IN {
        protected byte[] key;
        protected List<byte[]> values;

        public InWithValues(byte[] key, List<byte[]> values) {
            this.key = key;
            this.values = values;
        }

        public List<byte[]> values() {
            List<byte[]> buffers = new ArrayList<>(values.size());
            for (byte[] value : values)
                buffers.add(value);
            return buffers;
        }

        public boolean isSlice() {
            return false;
        }

        public boolean isEQ() {
            return false;
        }

        public boolean isIN() {
            return true;
        }

        public boolean isRange() {
            return false;
        }

        @Override
        public String toString() {
            String str = new String(key) + " in (";
            for (byte[] v : values)
                str += new String(v);
            str += ")";
            return str;
        }

        @Override
        public String genJSONString() {
            JSONObject json = new JSONObject();
            JSONArray array = new JSONArray();
            for (byte[] val : this.values()) {
                JSONObject jo = new JSONObject();
                jo.put("value", new String(val));
                array.add(jo);
            }
            json.put("type", "in");
            json.put("val", array);
            json.put("key", new String(key));
            return json.toString();
        }

        public static SingleRestriction.InWithValues parseJSONString(String json) {
            ArrayList<byte[]> vals = new ArrayList<>();
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);
            JSONArray value = (JSONArray) r.get("val");
            for (int i = 0; i < value.size(); i++) {
                String sval = (String) (((JSONObject) value.get(i)).get("value"));
                byte[] val = sval.getBytes();
                vals.add(val);
            }
            String key = (String) r.get("key");
            return new SingleRestriction.InWithValues(key.getBytes(), vals);
        }

        @Override
        public byte[] key() {
            return this.key;
        }

        @Override
        public boolean satisfy(byte[] val) {
            String v = new String(val);
            for (byte[] c : this.values()) {
                if (v.equalsIgnoreCase(new String(c))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void setValues(List<byte[]> v) {
            this.values = v;
        }

        @Override
        public JSONObject genJSON() {
            JSONObject json = new JSONObject();
            JSONArray array = new JSONArray();
            for (byte[] val : this.values()) {
                JSONObject jo = new JSONObject();
                jo.put("value", new String(val));
                array.add(jo);
            }
            json.put("type", "in");
            json.put("val", array);
            json.put("key", new String(key));
            return json;
        }

        public static SingleRestriction.InWithValues parseJSON(String json) {
            ArrayList<byte[]> vals = new ArrayList<>();
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);

            JSONArray value = (JSONArray) r.get("val");
            for (int i = 0; i < value.size(); i++) {
                String sval = (String) (((JSONObject) value.get(i)).get("value"));
                byte[] val = sval.getBytes();
                vals.add(val);
            }
            String key = (String) r.get("key");
            return new SingleRestriction.InWithValues(key.getBytes(), vals);
        }
    }

    public static class Range extends SingleRestriction implements Restriction.Range {

        protected byte[] key;
        protected byte[] starter;
        protected byte[] end;

        public Range(byte[] key, byte[] starter, byte[] end) {
            this.key = key;
            this.starter = starter;
            this.end = end;
        }

        @Override
        public boolean isSlice() {
            return false;
        }

        @Override
        public boolean isEQ() {
            return false;
        }

        @Override
        public boolean isIN() {
            return false;
        }

        @Override
        public boolean isRange() {
            return true;
        }

        @Override
        public byte[] key() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<byte[]> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            String str = new String(key) + " between [";
            str += new String(starter) + "," + new String(end);
            str += "]";
            return str;
        }

        public byte[] starter() {
            return this.starter;
        }

        public byte[] end() {
            return this.end;
        }

        @Override
        public boolean satisfy(byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String genJSONString() {
            JSONObject json = new JSONObject();
            json.put("type", "range");
            json.put("key", new String(key));
            json.put("starter", new String(starter));
            json.put("end", new String(end));
            return json.toString();
        }

        public static SingleRestriction.Range parseJSONString(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);

            byte[] key = ((String) r.get("key")).getBytes();
            String start = (String) r.get("starter");
            String end = (String) r.get("end");

            return new SingleRestriction.Range(key, start.getBytes(), end.getBytes());
        }

        @Override
        public void setValues(List<byte[]> v) {
            this.starter = v.get(0);
            this.end = v.get(1);
        }

        @Override
        public JSONObject genJSON() {
            JSONObject json = new JSONObject();
            json.put("type", "range");
            json.put("key", new String(key));
            json.put("starter", new String(starter));
            json.put("end", new String(end));
            return json;
        }

        public static SingleRestriction.Range parseJSON(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);

            byte[] key = ((String) r.get("key")).getBytes();
            String start = (String) r.get("starter");
            String end = (String) r.get("end");

            return new SingleRestriction.Range(key, start.getBytes(), end.getBytes());
        }

    }

    public static class Slice extends SingleRestriction implements Restriction.Slice {
        protected byte[] key;
        protected byte[] starter;
        protected byte[] end;
        protected boolean[] boundInclusive = new boolean[2];

        public Slice(byte[] key, byte[] start, byte[] end, boolean start_inclusive, boolean end_inclusive) {
            this.key = key;
            this.starter = start;
            this.end = end;
            boundInclusive[0] = start_inclusive;
            boundInclusive[1] = end_inclusive;
        }

        public boolean isSlice() {
            return true;
        }

        public boolean isEQ() {
            return false;
        }

        public boolean isIN() {
            return false;
        }

        public boolean isRange() {
            return false;
        }

        public List<byte[]> values() {
            throw new UnsupportedOperationException();
        }

        /**
         * Returns true if the start or end bound (depending on the argument) is set, false otherwise
         */
        public boolean hasBound(Bound b) {
            if (b == Bound.START)
                return this.starter != null;
            if (b == Bound.END)
                return this.end != null;
            return false;
        }

        public byte[] bound(Bound b) {
            return (b == Bound.START) ? starter : end;
        }

        /**
         * Returns true if the start or end bound (depending on the argument) is inclusive, false otherwise
         */
        public boolean isInclusive(Bound b) {
            return (b == Bound.START) ? (starter == null) : (end == null) || boundInclusive[b.idx];
        }

        public Relation.Type getRelation(Bound eocBound, Bound inclusiveBound) {
            switch (eocBound) {
                case START:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.GTE : Relation.Type.GT;
                case END:
                    return boundInclusive[inclusiveBound.idx] ? Relation.Type.LTE : Relation.Type.LT;
            }
            throw new AssertionError();
        }

        public void setBound(byte[] name, Relation.Type type, byte[] t) {
            Bound b;
            boolean inclusive;
            switch (type) {
                case GT:
                    b = Bound.START;
                    inclusive = false;
                    break;
                case GTE:
                    b = Bound.START;
                    inclusive = true;
                    break;
                case LT:
                    b = Bound.END;
                    inclusive = false;
                    break;
                case LTE:
                    b = Bound.END;
                    inclusive = true;
                    break;
                default:
                    throw new AssertionError();
            }

            boolean existed = (b == Bound.START) ? (starter != null) : (end != null);
            if (existed)
                System.out.println(String.format("More than one restriction was found for the %s bound on %s", b.name().toLowerCase(), name));
            //    throw new RequestException(String.format(
            //            "More than one restriction was found for the %s bound on %s", b.name().toLowerCase(), name));

            if (b == Bound.START)
                starter = t;
            else
                end = t;

            boundInclusive[b.idx] = inclusive;
        }

        @Override
        public String toString() {
            String str = new String(key) + " between [";
            str += new String(starter) + "," + new String(end);
            str += "]";
            return str;
        }

        @Override
        public String genJSONString() {
            JSONObject json = new JSONObject();
            json.put("type", "slice");
            json.put("key", new String(key));
            json.put("bound_start", new String(starter));
            json.put("bound_end", new String(end));
            json.put("start_inclusive", boundInclusive[0]);
            json.put("end_inclusive", boundInclusive[1]);
            return json.toString();
        }

        public static SingleRestriction.Slice parseJSONString(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);

            byte[] key = ((String) r.get("key")).getBytes();
            String start = (String) r.get("bound_start");
            String end = (String) r.get("bound_end");
            boolean start_inclusive = (boolean) r.get("start_inclusive");
            boolean end_inclusive = (boolean) r.get("end_inclusive");

            return new SingleRestriction.Slice(key, start.getBytes(), end.getBytes(), start_inclusive, end_inclusive);
        }

        @Override
        public byte[] key() {
            return this.key;
        }

        @Override
        public boolean satisfy(byte[] val) {
            String c = new String(val);
            String s = new String(starter);
            String e = new String(end);

            if (c.compareToIgnoreCase(s) > 0 && c.compareTo(e) < 0)
                return true;
            if (boundInclusive[0] && c.compareToIgnoreCase(s) == 0)
                return true;
            return boundInclusive[1] && c.compareToIgnoreCase(e) == 0;

        }

        @Override
        public void setValues(List<byte[]> v) {
            this.starter = v.get(0);
            this.end = v.get(1);
        }

        @Override
        public JSONObject genJSON() {
            JSONObject json = new JSONObject();
            json.put("type", "slice");
            json.put("key", new String(key));
            json.put("bound_start", new String(starter));
            json.put("bound_end", new String(end));
            json.put("start_inclusive", boundInclusive[0]);
            json.put("end_inclusive", boundInclusive[1]);
            return json;
        }

        public static SingleRestriction.Slice parseJSON(String json) {
            JSONCommand js = new JSONCommand();
            Map r = js.parse(json);

            byte[] key = ((String) r.get("key")).getBytes();
            String start = (String) r.get("bound_start");
            String end = (String) r.get("bound_end");
            boolean start_inclusive = (boolean) r.get("start_inclusive");
            boolean end_inclusive = (boolean) r.get("end_inclusive");

            return new SingleRestriction.Slice(key, start.getBytes(), end.getBytes(), start_inclusive, end_inclusive);
        }
    }
}
