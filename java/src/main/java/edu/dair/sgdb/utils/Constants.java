package edu.dair.sgdb.utils;

public class Constants {

    public static final int WORKER_THREAD_FACTOR = 10;

    public static final int THRIFT_POOL_SIZE = 128;
    public static final int Count_Threshold = 128;
    public static final int Threshold = 4096;
    public static final int K = 3;

    public final static int RTN_SUCC = 0;
    public static final int RTN_PAR = -1;

    public final static int RE_ACTUAL_LOC = 0;
    public final static int RE_VERTEX_WRONG_SRV = 1;
    public final static int EDGE_SPLIT_WRONG_SRV = 2;

    public final static int SPLIT_THRESHOLD = 60000;
    public final static int REASSIGN_THRESHOLD = 600;

    public static int RETRY = 3;

    public static int LIMITS = 1 * 1024 * 1024; //1M

    public static final int MAX_RADIX = 11;
    public static final int BITS_PER_MAP = 8;
    public static final int MAX_VIRTUAL_NODE = (1 << (MAX_RADIX - 1)); // Totally 1024 Virtual Nodes.
    public static final int MAX_BMAP_LEN = (1 << MAX_RADIX); //2048
    //public static final int MAX_GIGA_PARTITIONS = (1 << MAX_RADIX);

    public static final String DB_META = "DB_META";

    public static final int SPLIT_START = 0;
    public static final int SPLIT_END = 1;
    public static final int REPORT_SPLIT = 2;
}
