package edu.dair.sgdb.partitioner;

import edu.dair.sgdb.utils.ArrayPrimitives;
import edu.dair.sgdb.utils.Constants;
import edu.dair.sgdb.utils.GLogger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class DIDOIndex {

    //all GigaIndex share the same virtmap, which is the tree structure.
    public static Short[] virtmap = new Short[Constants.MAX_BMAP_LEN];
    //virtual node -> Index; assume virtual node starts from 0
    public static Short[] virt2index = new Short[Constants.MAX_VIRTUAL_NODE];

    // To construct a tree structure with virtmap.
    // As for the vertmap, the value would be the virtual node offset.
    // As for the vert2index, the index would be the serverID, the value would be the virtual node offset.
    //
    static {
        virtmap[0] = 0;
        virt2index[0] = 0;
        short max = virtmap[0];
        for (int i = 1; i < Constants.MAX_BMAP_LEN; i++) {
            if (i % 2 == 1) {
                virtmap[i] = virtmap[(i - 1) / 2];
            } else {
                virtmap[i] = (short) ((max + 1) % Constants.MAX_VIRTUAL_NODE);
                max = virtmap[i];
            }
            virt2index[virtmap[i]] = (short) i;
        }
    }

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
        GLogger.info("[%d]: BitMap: %s", id, t);
    }

    // bitmap is of length 256, since it is stored as a byte array.
    public byte[] bitmap = new byte[Constants.MAX_BMAP_LEN / 8];
    public HashMap<Short, VirtualNodeStatus> virt;
    public int serverNum;
    public int startServer;

    public DIDOIndex(int startServer, int serverNum) {
        this.startServer = startServer;
        this.serverNum = serverNum;
        this.virt = new HashMap<>();
        this.bitmap[0] = (byte) (1 << 7);
    }

    public DIDOIndex(byte[] byteArray) {
        virt = new HashMap<>();

        int offset = 0;
        System.arraycopy(byteArray, offset, this.bitmap, 0, this.bitmap.length);
        offset += bitmap.length;

        this.startServer = ArrayPrimitives.btoi(byteArray, offset);
        offset += 4;

        this.serverNum = ArrayPrimitives.btoi(byteArray, offset);

    }

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
                virt.put((short) vnode, new VirtualNodeStatus((short) 0, (short) 0));
            }
            return virt.get((short) vnode);
        }
    }

    public void add_vid_count(int vnode) {
        synchronized (virt) {
            if (!virt.containsKey((short) vnode)) {
                virt.put((short) vnode, new VirtualNodeStatus((short) 0, (short) 0));
            }
            virt.get((short) vnode).size += 1;
        }
    }

    public int giga_bitmap_compare(byte[] bm) {
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

    public void giga_update_bitmap(byte[] update) {
        synchronized (this.bitmap) {
            for (int i = 0; i < Constants.MAX_BMAP_LEN / 8; i++) {
                bitmap[i] = (byte) (bitmap[i] | update[i]);
            }
        }
        return;
    }

    public int giga_get_index_for_hash(int hash) {
        synchronized (this.bitmap) {
            return compute_index(hash);
        }
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

    public int giga_get_server_for_hash(int hash) {
        int index = giga_get_index_for_hash(hash);
        return giga_get_server_from_index(index);
    }

    public int giga_get_vid_from_hash(int hash) {
        int index = giga_get_index_for_hash(hash);
        return giga_get_vid_from_index(index);
    }

    public int giga_get_vid_from_index(int index) {
        return (DIDOIndex.virtmap[index] + this.startServer) % Constants.MAX_VIRTUAL_NODE;
    }

    public int giga_get_server_from_index(int index) {
        return ((DIDOIndex.virtmap[index] + this.startServer) % Constants.MAX_VIRTUAL_NODE) % serverNum;
    }

    public int get_radix_from_bmap() {
        int radix = get_radix_from_index(get_highest_index());
        return radix;
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

    public int get_highest_index() {
        int i, j;
        int max_index = -1;
        int index_in_bmap = -1;  // highest index with a non-zero element
        int bit_in_index = -1;  // highest bit in the element in index_in_bmap
        //
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
        assert (max_index >= 0);
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
        int vid = hash % Constants.MAX_VIRTUAL_NODE;
        int virtual_offset = ((vid - this.startServer) < 0)
                ? (Constants.MAX_VIRTUAL_NODE + vid - this.startServer)
                : (vid - this.startServer);

        int index = DIDOIndex.virt2index[virtual_offset];
        while (!get_bit_status(index) && index > 0) {
            index = this.get_parent_index(index);
        }
        return index;
    }

    public boolean contains(int idx, int vid) {
        if (idx >= Constants.MAX_BMAP_LEN) {
            return false;
        }

        if (((DIDOIndex.virtmap[idx] + this.startServer) % Constants.MAX_VIRTUAL_NODE) == vid) {
            return true;
        }

        int left = 2 * idx + 1;
        int right = 2 * idx + 2;
        return contains(left, vid) || contains(right, vid);
    }

    public boolean giga_should_move(int distHash, int index) {
        int vid = distHash % Constants.MAX_VIRTUAL_NODE;
        //int left = 2 * index + 1;
        return contains(index, vid);
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(virt2index));
        System.out.println(Arrays.toString(virtmap));
    }

    public class VirtualNodeStatus {

        public short size;

        private byte split_to;
        private byte split_from;
        private byte has_split;

        public short countsWhileSplitting;

        public VirtualNodeStatus(short size, short counts) {
            this.size = size;
            this.split_to = (byte) 0;
            this.split_from = (byte) 0;
            this.has_split = (byte) 0;
            this.countsWhileSplitting = counts;
        }

        public VirtualNodeStatus(byte[] array) {
            this.size = ArrayPrimitives.btos(array, 0);
            this.split_to = array[2];
            this.split_from = array[3];
            this.has_split = array[4];
            this.countsWhileSplitting = ArrayPrimitives.btos(array, 5);
        }

        public byte[] toByteArray() {
            byte[] array = new byte[2 + 3 + 2];
            System.arraycopy(ArrayPrimitives.stob(size), 0, array, 0, 2);
            array[2] = split_to;
            array[3] = split_from;
            array[4] = has_split;
            System.arraycopy(ArrayPrimitives.stob((short) countsWhileSplitting), 0, array, 5, 2);
            return array;
        }

        public boolean has_split() {
            return (has_split == (byte) 1);
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

        public void split_to() {
            this.split_to = (byte) 1;
            this.has_split = (byte) 1;
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


}
