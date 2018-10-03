package edu.dair.sgdb.sengine;

import edu.dair.sgdb.thrift.KeyValue;
import edu.dair.sgdb.utils.Constants;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;


public class OrderedRocksDBAPI {

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Memcmp should first consider the key size; longer byte[] is always bigger
     * than shorter one For example, "vertex1000" > "vertex9"; Please note, this
     * is different from c lib's memcmp;
     */
    private int memcmp(byte[] buffer1, byte[] buffer2) {
        int length1 = buffer1.length;
        int length2 = buffer2.length;

        if (buffer1 == buffer2 && length1 == length2) {
            return 0;
        }
        if (length1 != length2) {
            return length1 - length2;
        }

        for (int i = 0, j = 0; i < length1; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return 0;
    }

    public class GraphFSComparator extends Comparator {

        public GraphFSComparator(ComparatorOptions co) {
            super(co);
        }

        @Override
        public String name() {
            return "GraphFSComparator";
        }

        /**
         * Compare two keys: src-id (ByteBuffer) : edge-type (int) : dst-id
         * (ByteBuffer)
         */
        @Override
        public int compare(Slice a, Slice b) {
            DBKey keyA = new DBKey(a.data());
            DBKey keyB = new DBKey(b.data());

            int srcc = memcmp(keyA.src, keyB.src);
            if (srcc != 0) {
                return srcc;
            }
            if (keyA.type != keyB.type) {
                return (keyA.type - keyB.type);
            }

            int dstc = memcmp(keyA.dst, keyB.dst);
            if (dstc != 0) {
                return dstc;
            }

            return 0;
        }
    }

    private Options options;
    private RocksDB db = null;

    public OrderedRocksDBAPI(String dbfile) {
        try {
            this.options = new Options().setCreateIfMissing(true);
            options.setAllowOsBuffer(true);
            options.setMaxWriteBufferNumber(4);
            options.setMinWriteBufferNumberToMerge(2);
            options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
            options.getEnv().setBackgroundThreads(8, RocksEnv.COMPACTION_POOL);
            options.getEnv().setBackgroundThreads(2, RocksEnv.FLUSH_POOL);
            options.setDisableAutoCompactions(true);
            options.setMaxBackgroundCompactions(8);
            options.setMaxBackgroundFlushes(2);
            options.setCompactionStyle(CompactionStyle.LEVEL);
            options.setLevelZeroFileNumCompactionTrigger(10);
            options.setLevelZeroSlowdownWritesTrigger(20);
            options.setLevelZeroStopWritesTrigger(40);
            options.setWriteBufferSize(67108864L); //33554432
            options.setTargetFileSizeBase(67108864L);
            options.setMaxBytesForLevelBase(671088640L);
            options.setMaxBytesForLevelMultiplier(10);
            options.setVerifyChecksumsInCompaction(false);
            options.setMaxOpenFiles(-1);

            Filter bloomFilter = new BloomFilter(10); // 10 bits per key = ~ 1% false positive;
            BlockBasedTableConfig bbTableConfig = new BlockBasedTableConfig();
            bbTableConfig.setIndexType(org.rocksdb.IndexType.kBinarySearch);
            //accroding to https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide, HashSearch add
            //5% space overhead, but 50% random read performance improvement
            //bbTableConfig.setIndexType(IndexType.kHashSearch);
            //options.useFixedLengthPrefixExtractor(10);
            bbTableConfig.setBlockSize(1024L);
            bbTableConfig.setCacheIndexAndFilterBlocks(true);
            bbTableConfig.setFilter(bloomFilter);
            options.setTableFormatConfig(bbTableConfig);

            ComparatorOptions co = new ComparatorOptions();
            GraphFSComparator cmp = new GraphFSComparator(co);
            options.setComparator(cmp);
            this.db = RocksDB.open(options, dbfile);
        } catch (RocksDBException e) {
            System.out.println("RocksDB Open Error");
            e.printStackTrace();
        }
    }

    public void close() {
        if (db != null) {
            this.db.close();
        }
        this.options.dispose();
    }

    public byte[] get(byte[] key) {
        byte[] val;
        try {
            val = this.db.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        return val;
    }

    public void put(byte[] key, byte[] val) {
        if (val != null) {
            try {
                this.db.put(key, val);
            } catch (RocksDBException e) {
                e.printStackTrace();
                return;
            }
        }
        return;
    }

    public void batch_put(List<KeyValue> kvs) {
        WriteBatch batch = new WriteBatch();
        WriteOptions options = new WriteOptions();
        for (KeyValue kv : kvs) {
            batch.put(kv.getKey(), kv.getValue());
        }

        try {
            this.db.write(options, batch);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private int compareKey(byte[] a, byte[] b) {
        DBKey keyA = new DBKey(a);
        DBKey keyB = new DBKey(b);
        int srcc = memcmp(keyA.src, keyB.src);
        if (srcc != 0) {
            return srcc;
        }
        if (keyA.type != keyB.type) {
            return (keyA.type - keyB.type);
        }

        int dstc = memcmp(keyA.dst, keyB.dst);
        if (dstc != 0) {
            return dstc;
        }

        return 0;
    }

    public ArrayList<byte[]> scan(byte[] start, byte[] end) {
        ArrayList<byte[]> rtn = new ArrayList<>();
        RocksIterator ri = this.db.newIterator();
        ri.seek(start);
        byte[] sKey = ri.key();

        while (ri.isValid() && compareKey(sKey, end) < 0) {
            rtn.add(ri.value());
            ri.next();
            sKey = ri.key();
        }
        return rtn;
    }

    public ArrayList<byte[]> scanKey(byte[] start, byte[] end) {
        ArrayList<byte[]> rtn = new ArrayList<byte[]>();

        RocksIterator ri = this.db.newIterator();
        ri.seek(start);
        byte[] sKey = ri.key();

        while (ri.isValid() && compareKey(sKey, end) < 0) {
            rtn.add(ri.key());
            ri.next();
            sKey = ri.key();
        }
        return rtn;
    }

    public ArrayList<KeyValue> scanKV(byte[] start, byte[] end) {
        ArrayList<KeyValue> rtn = new ArrayList<>();
        RocksIterator ri = this.db.newIterator();
        ri.seek(start);

        while (ri.isValid() && compareKey(ri.key(), end) <= 0) {
            KeyValue kv = new KeyValue();
            kv.setKey(ri.key()).setValue(ri.value());
            rtn.add(kv);
            ri.next();
        }
        return rtn;
    }

    public byte[] scanLimitedRes(byte[] start, byte[] end, int limits, ArrayList<KeyValue> rtn) {
        RocksIterator ri = this.db.newIterator();
        ri.seek(start);
        byte[] sKey = ri.key();
        int scanned = 0;

        while (ri.isValid() && compareKey(sKey, end) <= 0 && scanned < limits) {
            if (compareKey(sKey, start) > 0) {
                KeyValue kv = new KeyValue();
                kv.setKey(ri.key()).setValue(ri.value());
                scanned += (ri.key().length + ri.value().length);
                rtn.add(kv);
            }
            ri.next();
            sKey = ri.key();
        }
        if (rtn.size() == 0) {
            return null;
        }
        return sKey;
    }

    public void remove(byte[] key) {
        try {
            this.db.remove(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return;
        }
        return;
    }

    public KeyValue seekTo(byte[] pivot) {
        KeyValue rtn = new KeyValue();
        RocksIterator ri = this.db.newIterator();
        ri.seek(pivot);

        if (!ri.isValid()) {
            ri.next();
        }
        //TODO: Have to consider the situation that users try to read a non-exist entry
        if (ri.isValid()) {
            rtn.setKey(ri.key()).setValue(ri.value());
            return rtn;
        }
        return null;
    }

    public void testRun() {
        RocksIterator it = this.db.newIterator();
        int i = 0;
        DBKey minDBMeta = DBKey.MinDBKey(Constants.DB_META.getBytes(), 0);
        for (byte b : minDBMeta.toKey()) {
            System.out.print(b + " ");
        }
        System.out.println("");
        for (it.seek(minDBMeta.toKey()); it.isValid(); it.next()) {
            byte[] key = it.key();
            for (byte a : key) {
                System.out.print(a + " ");
            }
            i++;
            System.out.println("");
        }
        System.out.println("num: " + i);
        /*
         byte[] MAX_BYTE = {(byte) 0xFF};
         byte[] NORMAL_BYTE = {(byte) 0x7F};
         System.out.println("compare: " + this.memcmp(MAX_BYTE, NORMAL_BYTE));


        String payload256 = "";
        for (int i = 0; i < 128; i++) {
            payload256 += "a";
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            int type = 1;
            long ts = System.currentTimeMillis();
            Random r = new Random();
            int i1 = Math.abs(r.nextInt());
            int i2 = Math.abs(r.nextInt());
            byte[] src = ArrayPrimitives.itob(i1);
            byte[] dst = ArrayPrimitives.itob(i2);

            DBKey k = new DBKey(src, dst, type, ts);
            //System.out.println("insert: " + k);
            byte[] val = ("val" + i + payload256).getBytes();
            put(k.toKey(), val);
        }
        System.out.println("insert time: " + (System.currentTimeMillis() - start) + " ms");
        */
        /*
         System.out.println("start scan");

         //DBKey e = new DBKey(ArrayPrimitives.itob(100), ArrayPrimitives.itob(50), 1, 0L);
         //DBKey s = new DBKey(ArrayPrimitives.itob(100), ArrayPrimitives.itob(50), 1, Long.MAX_VALUE);
         DBKey s = DBKey.MinDBKey(ArrayPrimitives.itob(100), 1, Long.MAX_VALUE);
         DBKey e = DBKey.MaxDBKey(ArrayPrimitives.itob(100), 1, 0);
         byte[] startKey = s.toKey();
         byte[] endKey = e.toKey();
         ArrayList<KeyValue> vals = scanKV(startKey, endKey);
         System.out.println("vals: " + vals.size());
         for (KeyValue p : vals){
         byte[] k = p.getKey();
         DBKey key = new DBKey(k);
         System.out.println(key);
         }
         */
        this.close();

    }

    public static void main(String[] args) {
        OrderedRocksDBAPI test = new OrderedRocksDBAPI("/tmp/gfs0");
        test.testRun();
    }

}
