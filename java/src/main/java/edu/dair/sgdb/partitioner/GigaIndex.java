package edu.dair.sgdb.partitioner;

import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;

import java.util.HashMap;
import java.util.HashSet;

public class GigaIndex {
    public class VirtualNodeStatus {

        public short size;
        public byte isSplit;

        private byte split_to;
        private byte split_from;
        private byte has_split;

        public Short countsWhileSplitting;

        public VirtualNodeStatus(short size) {
            this.size = size;
            this.isSplit = (byte) 0;
            this.has_split = (byte) 0;
            this.countsWhileSplitting = 0;
        }

        public VirtualNodeStatus(byte[] array) {
            this.size = ArrayPrimitives.btos(array, 0);
            this.isSplit = array[2];
            this.split_to = array[3];
            this.split_from = array[4];
            this.has_split = array[5];
            this.countsWhileSplitting = ArrayPrimitives.btos(array, 6);

        }

        public byte[] toByteArray() {
            byte[] array = new byte[2 + 5 + 2];
            System.arraycopy(ArrayPrimitives.stob(size), 0, array, 0, 2);
            array[2] = this.isSplit;
            array[3] = split_to;
            array[4] = split_from;
            array[5] = has_split;
            System.arraycopy(ArrayPrimitives.stob((short) countsWhileSplitting), 0, array, 6, 2);

            return array;
        }

        public boolean has_split() {
            return (has_split == (byte) 1);
        }

        public synchronized boolean needSplit() {
            return (size >= Constants.Count_Threshold && this.isSplit == (byte) 0);
        }

        public synchronized int incr_size() {
            size += 1;
            return size;
        }

        public synchronized int incr_size(int s) {
            size += s;
            return size;
        }

        public synchronized int decr_size(int s) {
            size -= s;
            return size;
        }

        public synchronized int halv_size() {
            size /= 2;
            return size;
        }

        public void split() {
            this.isSplit = (byte) 1;
            this.has_split = (byte) 1;
        }

        public void finishSplit() {
            this.isSplit = (byte) 0;
        }

        public void split_to() {
            this.split_to = (byte) 1;
        }

        public void finish_split_to() {
            this.split_to = (byte) 0;
        }

        public boolean is_splitting_to() {
            return (this.split_to == (byte) 1);
        }

        public void split_from() {
            this.split_from = (byte) 1;
        }

        public void finish_split_from() {
            this.split_from = (byte) 0;
        }

        public boolean is_splitting_from() {
            return (this.split_from == (byte) 1);
        }

        public synchronized int incr_counter() {
            return ++countsWhileSplitting;
        }

        public synchronized int reset_counter() {
            countsWhileSplitting = 0;
            return 0;
        }

    }

    public static Short[] virtmap = new Short[Constants.MAX_BMAP_LEN]; //all GigaIndex share the same virtmap;

    static {
        virtmap[0] = 0;
        short max = virtmap[0];
        for (int i = 1; i < Constants.MAX_BMAP_LEN; i++) {
            if (i % 2 == 1) {
                virtmap[i] = virtmap[(i - 1) / 2];
            } else {
                virtmap[i] = (short) ((max + 1) % Constants.MAX_VIRTUAL_NODE);
                max = virtmap[i];
            }
        }
    }

    public byte[] bitmap = new byte[Constants.MAX_BMAP_LEN / 8];
    public HashMap<Short, VirtualNodeStatus> virt;
    public int serverNum;
    public int startServer;

    public GigaIndex(int startServer, int serverNum) {
        this.startServer = startServer;
        this.serverNum = serverNum;
        this.bitmap[0] = (byte) (1 << 7);
        virt = new HashMap<>();
    }

    public GigaIndex(byte[] byteArray) {
        virt = new HashMap<>();

        int offset = 0;
        System.arraycopy(byteArray, offset, this.bitmap, 0, this.bitmap.length);
        offset += bitmap.length;

        this.startServer = ArrayPrimitives.btoi(byteArray, offset);
        offset += 4;

        this.serverNum = ArrayPrimitives.btoi(byteArray, offset);
    }

    // do not serialize virtual node status as it can be rebuilt by scanning the whole db.
    public byte[] toByteArray() {
        int size = 0;
        size += bitmap.length;
        size += (4 + 4);  //serverNum, startServer
        size += 4; //virt size
        size += (virt.size() * 8);
        byte[] array = new byte[size];

        int offset = 0;
        System.arraycopy(bitmap, 0, array, offset, bitmap.length);
        offset += bitmap.length;

        System.arraycopy(ArrayPrimitives.itob(startServer), 0, array, offset, 4);
        offset += 4;

        System.arraycopy(ArrayPrimitives.itob(serverNum), 0, array, offset, 4);

        return array;
    }

    public VirtualNodeStatus surelyGetVirtNodeStatus(int vnode) {
        synchronized (virt) {
            if (!virt.containsKey((short) vnode)) {
                virt.put((short) vnode, new VirtualNodeStatus((short) 0));
            }
            return virt.get((short) vnode);
        }
    }

    public void add_vid_count(int vnode) {
        synchronized (virt) {
            if (!virt.containsKey((short) vnode)) {
                virt.put((short) vnode, new VirtualNodeStatus((short) 0));
            }
            virt.get((short) vnode).size += 1;
        }
    }

    public int giga_bitmap_compare(byte[] bm) {
        synchronized (this.bitmap) {
            for (int i = 0; i < Constants.MAX_BMAP_LEN / 8; i++) {
                if (bitmap[i] == bm[i]) {
                    continue;
                }
                byte c = (byte) ((byte) (bitmap[i] | bm[i]) & (~bitmap[i]));
                if (c == 0) {
                    return 1;
                } else {
                    return -1;
                }
            }
            return 0;
        }
    }

    public boolean giga_can_split(int index) {
        return (get_radix_from_index(index) < (Constants.MAX_RADIX - 1));
    }

    public int giga_split_mapping(int index) {
        synchronized (this.bitmap) {
            if (!get_bit_status(index)) {
                return -1;
            }

            int i = get_radix_from_index(index);
            int new_index = 0;
            while (i < Constants.MAX_RADIX) {
                new_index = get_child_index(index, i);
                if (!get_bit_status(new_index)) {
                    break;
                }
                i++;
            }
            int rtn = new_index + 1;

            int index_in_bmap = new_index / Constants.BITS_PER_MAP;
            int bit_in_index = new_index % Constants.BITS_PER_MAP;
            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            byte bitInfo = bitmap[index_in_bmap];
            bitInfo = (byte) (bitInfo | mask);
            bitmap[index_in_bmap] = bitInfo;

            index_in_bmap = (new_index + 1) / Constants.BITS_PER_MAP;
            bit_in_index = (new_index + 1) % Constants.BITS_PER_MAP;
            mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            bitInfo = bitmap[index_in_bmap];
            bitInfo = (byte) (bitInfo | mask);
            bitmap[index_in_bmap] = bitInfo;

            return rtn;
        }
    }

    public void giga_split_update_mapping(int index, int new_index) {
        synchronized (this.bitmap) {
            int index_in_bmap = new_index / Constants.BITS_PER_MAP;
            int bit_in_index = new_index % Constants.BITS_PER_MAP;
            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            byte bitInfo = bitmap[index_in_bmap];
            bitInfo = (byte) (bitInfo | mask);
            bitmap[index_in_bmap] = bitInfo;

            index_in_bmap = (new_index + 1) / Constants.BITS_PER_MAP;
            bit_in_index = (new_index + 1) % Constants.BITS_PER_MAP;
            mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            bitInfo = bitmap[index_in_bmap];
            bitInfo = (byte) (bitInfo | mask);
            bitmap[index_in_bmap] = bitInfo;
        }
    }

    public int giga_split_new_index(int index) {
        synchronized (this.bitmap) {
            if (!get_bit_status(index)) {
                return -1;
            }

            int i = get_radix_from_index(index);
            int new_index = 0;
            while (i < Constants.MAX_RADIX) {
                new_index = get_child_index(index, i);
                if (!get_bit_status(new_index)) {
                    break;
                }
                i++;
            }
            int rtn = new_index + 1;
            return rtn;
        }
    }

    public void giga_update_bitmap(byte[] update) {
        synchronized (this.bitmap) {
            for (int i = 0; i < Constants.MAX_BMAP_LEN / 8; i++) {
                bitmap[i] = (byte) (bitmap[i] | update[i]);
            }
        }
        return;
    }

    public HashSet<Integer> giga_get_all_virt_nodes() {
        HashSet<Integer> vnodes = new HashSet<>();
        synchronized (this.bitmap) {
            int highest = this.get_highest_index();
            for (int i = highest; i >= 0; i--) {
                if (get_bit_status(i)) {
                    vnodes.add(giga_get_vid_from_index(i));
                }
            }
        }
        return vnodes;
    }

    public HashSet<Integer> giga_get_all_servers() {
        HashSet<Integer> srvs = new HashSet<>();
        synchronized (this.bitmap) {
            for (int i = 0; i < Constants.MAX_BMAP_LEN; i += 2) {
                if (get_bit_status(i)) {
                    srvs.add(giga_get_server_from_index(i));
                }
            }
        }
        return srvs;
    }

    public int giga_get_index_for_hash(int hash) {
        synchronized (this.bitmap) {
            return compute_index(hash);
        }
    }

    public int giga_get_server_from_hash(int hash) {
        int index = giga_get_index_for_hash(hash);
        return giga_get_server_from_index(index);
    }

    public int giga_get_vid_from_hash(int hash) {
        int index = giga_get_index_for_hash(hash);
        return giga_get_vid_from_index(index);
    }

    public int giga_get_vid_from_index(int index) {
        return (GigaIndex.virtmap[index] + this.startServer) % Constants.MAX_VIRTUAL_NODE;
    }

    public int giga_get_server_from_index(int index) {
        return ((GigaIndex.virtmap[index] + this.startServer) % Constants.MAX_VIRTUAL_NODE) % serverNum;
    }

    public int get_radix_from_bmap() {
        int radix = get_radix_from_index(get_highest_index());
        return radix;
    }

    public int get_highest_index() {
        int i, j;
        int max_index = -1;
        int index_in_bmap = -1;  // highest index with a non-zero element
        int bit_in_index = -1;  // highest bit in the element in index_in_bmap

        for (i = (Constants.MAX_BMAP_LEN / 8) - 1; i >= 0; i--) {
            if (bitmap[i] != 0) {
                index_in_bmap = i;
                break;
            }
        }

        byte value = bitmap[i];
        for (j = Constants.BITS_PER_MAP - 1; j >= 0; j--) {
            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - j));
            if ((value & mask) != 0) {
                bit_in_index = j;  //reversed byte: [01234567]
                break;
            }
        }

        max_index = (index_in_bmap * Constants.BITS_PER_MAP) + bit_in_index;
        return max_index;
    }

    private double log2(double x) {
        return Math.log(x) / Math.log(2);
    }

    public int get_radix_from_index(int index) {
        return ((int) (log2(index + 1.0)));
    }

    public boolean get_bit_status(int index) {
        int index_in_bmap = index / Constants.BITS_PER_MAP;
        int bit_in_index = index % Constants.BITS_PER_MAP;

        byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
        byte bit_info = this.bitmap[index_in_bmap];

        bit_info = (byte) (bit_info & mask);
        return bit_info != 0;
    }

    public int get_child_index(int index, int radix) {
        return 2 * index + 1;
    }

    public int get_parent_index(int index) {
        int parent_index = 0;
        if (index > 0) {
            parent_index = (index - 1) / 2;
        }
        return parent_index;
    }

    public int compute_index(int hash) {
        int index = 0;
        double hashRange = (hash + 0.0) / (Integer.MAX_VALUE + 0.0); //
        int highest = this.get_highest_index();
        for (index = highest; index >= 0; index--) {
            if (get_bit_status(index)) {
                int currRadix = get_radix_from_index(index);
                int orderInRadix = 0;
                if (currRadix > 0) {
                    orderInRadix = index - ((1 << currRadix) - 1);
                }

                double rs = 0 + (1.0 / ((1 << currRadix) + 0.0)) * orderInRadix;
                double re = (1.0 / ((1 << currRadix) + 0.0)) * (orderInRadix + 1);
                if (hashRange < re && hashRange >= rs) {
                    break;
                }
            }
        }
        return index;
    }

    public boolean giga_should_split(int hash, int index) {
        double hashRange = (hash + 0.0) / (Integer.MAX_VALUE + 0.0);
        if (!get_bit_status(index)) {
            return false;
        }
        int currRadix = get_radix_from_index(index);
        int orderInRadix = 0;
        if (currRadix > 0) {
            orderInRadix = index - ((1 << currRadix) - 1);
        }

        double rs = 0 + (1.0 / ((1 << currRadix) + 0.0)) * orderInRadix;
        double re = (1.0 / ((1 << currRadix) + 0.0)) * (orderInRadix + 1);
        return hashRange < re && hashRange >= rs;
    }

    public int getSplitCounter() {
        int highest = this.get_highest_index();
        int counter = 0;
        for (int index = highest; index > 0; index--) {
            if (get_bit_status(index)) {
                counter++;
            }
        }
        return counter / 2;
    }

    /**
     * TEST
     */
    public static int get_higest(byte[] bitmap) {
        int i, j;
        int max_index = -1;
        int index_in_bmap = -1;  // highest index with a non-zero element
        int bit_in_index = -1;  // highest bit in the element in index_in_bmap

        for (i = (Constants.MAX_BMAP_LEN / 8) - 1; i >= 0; i--) {
            if (bitmap[i] != 0) {
                index_in_bmap = i;
                break;
            }
        }

        byte value = bitmap[i];
        for (j = Constants.BITS_PER_MAP - 1; j >= 0; j--) {
            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - j));
            if ((value & mask) != 0) {
                bit_in_index = j;
                break;
            }
        }

        max_index = (index_in_bmap * Constants.BITS_PER_MAP) + bit_in_index;
        return max_index;
    }

    public static void pprint(int id, byte[] bitmap) {
        String t = "";
        int max = get_higest(bitmap);
        for (int index = 0; index <= max; index++) {
            int index_in_bmap = index / Constants.BITS_PER_MAP;
            int bit_in_index = index % Constants.BITS_PER_MAP;

            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            byte bit_info = bitmap[index_in_bmap];

            bit_info = (byte) (bit_info & mask);
            if (bit_info != 0) {
                t += "1";
            } else {
                t += "0";
            }
        }
        System.out.println("[" + id + "]: BitMap: " + t);
        GLogger.info("[%d]: BitMap: %s", id, t);
    }

    public static void pprint(byte[] bitmap) {
        int max = get_higest(bitmap);
        for (int index = 0; index <= max; index++) {
            int index_in_bmap = index / Constants.BITS_PER_MAP;
            int bit_in_index = index % Constants.BITS_PER_MAP;

            byte mask = (byte) (1 << (Constants.BITS_PER_MAP - 1 - bit_in_index));
            byte bit_info = bitmap[index_in_bmap];

            bit_info = (byte) (bit_info & mask);
            if (bit_info != 0) {
                System.out.print("1");
            } else {
                System.out.print("0");
            }
        }
        System.out.println();
    }

    /*
     public static void main(String[] args) {
     GigaIndex g = new GigaIndex(1, 4);
     GigaIndex.pprint(g.bitmap);
     HashSet<Integer> srvs = g.giga_get_all_servers();
     System.out.println("srvs: " + srvs);

     //g.giga_split_mapping(0);
     GigaIndex.pprint(g.bitmap);
     byte[] bm = new byte[g.bitmap.length];
     System.arraycopy(g.bitmap, 0, bm, 0, g.bitmap.length);
     HashSet<Integer> srvs1 = g.giga_get_all_servers();
     System.out.println("srvs1: " + srvs1);

     g.giga_split_mapping(2);
     GigaIndex.pprint(g.bitmap);
     HashSet<Integer> srvs2 = g.giga_get_all_servers();
     System.out.println("srvs2: " + srvs2);

     g.giga_split_mapping(5);
     GigaIndex.pprint(g.bitmap);
     HashSet<Integer> srvs3 = g.giga_get_all_servers();
     System.out.println("srvs3: " + srvs3);

     System.out.println("Compare: " + g.giga_bitmap_compare(bm));
     //int radix = get_radix_from_index(get_highest_index());

     System.out.println("Highest index: " + g.get_highest_index());
     System.out.println("Radix: " + g.get_radix_from_bmap());

     for (int i = 50; i >= 0; i--) {
     Random r = new Random();
     int v = Math.abs(r.nextInt());
     int index = g.giga_get_index_for_hash(v);
     int vid = (GigaIndex.virtmap[index] + g.startServer) % Constants.MAX_VIRTUAL_NODE;
     System.out.println("i: " + v + " index: " + index + " virtual Id: " + vid);
     }
     }
     */
}
