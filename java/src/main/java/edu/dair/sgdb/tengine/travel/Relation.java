package edu.dair.sgdb.tengine.travel;

/**
 * This code is partially from Cassandra.
 *
 * @author daidong
 */

public abstract class Relation {

    protected Type relationType;

    public enum Type {
        EQ, LT, LTE, GTE, GT, IN, CONTAINS, CONTAINS_KEY, NEQ, SLICE, RANGE;

        public boolean allowsIndexQuery() {
            switch (this) {
                case EQ:
                case CONTAINS:
                case CONTAINS_KEY:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String toString() {
            switch (this) {
                case EQ:
                    return "=";
                case LT:
                    return "<";
                case LTE:
                    return "<=";
                case GT:
                    return ">";
                case GTE:
                    return ">=";
                case NEQ:
                    return "!=";
                default:
                    return this.name();
            }
        }
    }

    public Type operator() {
        return relationType;
    }

}
