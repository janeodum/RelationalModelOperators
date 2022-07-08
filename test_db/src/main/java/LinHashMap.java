
/************************************************************************************
 * @file LinHashMap.java
 *
 * @author  John Miller
 */

import java.io.*;
import java.lang.reflect.Array;
import static java.lang.System.out;
import java.util.*;

/************************************************************************************
 * This class provides hash maps that use the Linear Hashing algorithm.
 * A hash table is created that is an expandable array-list of buckets.
 */
public class LinHashMap <K, V>
       extends AbstractMap <K, V>
       implements Serializable, Cloneable, Map <K, V>
{
    /** The debug flag
     */
    private static final boolean DEBUG = true;

    /** The number of slots (for key-value pairs) per bucket.
     */
    private static final int SLOTS = 4;

    /** The threshold/upper bound on the load factor
     */
    private static final double THRESHOLD = 1.2;

    /** The class for type K.
     */
    private final Class <K> classK;

    /** The class for type V.
     */
    private final Class <V> classV;

    /********************************************************************************
     * This inner class defines buckets that are stored in the hash table.
     */
    private class Bucket
    {
        int    nKeys;
        K []   key;
        V []   value;
        Bucket next;

        @SuppressWarnings("unchecked")
        Bucket ()
        {
            nKeys = 0;
            key   = (K []) Array.newInstance (classK, SLOTS);
            value = (V []) Array.newInstance (classV, SLOTS);
            next  = null;

        } // constructor

        V find (K k)
        {
            for (int j = 0; j < nKeys; j++) if (key[j].equals (k)) return value[j];
            return null;
        } // find

        void add (K k, V v)
        {
            key[nKeys]   = k;
            value[nKeys] = v;
            nKeys++;
        } // add

        void print ()
        {
            out.print ("[ " );
            for (int j = 0; j < nKeys; j++) out.print (key[j] + " . ");
            out.println ("]" );
        } // print

    } // Bucket inner class

    /** The list of buckets making up the hash table.
     */
    private final List <Bucket> hTable;

    /** The modulus for low resolution hashing
     */
    private int mod1;

    /** The modulus for high resolution hashing
     */
    private int mod2;

    /** The index of the next bucket to split.
     */
    private int isplit = 0;

    /** Counter for the number buckets accessed (for performance testing).
     */
    private int count = 0;

    /** The counter for the total number of keys in the LinHash Map
     */
    private int keyCount = 0;

    /********************************************************************************
     * Construct a hash table that uses Linear Hashing.
     * @param _classK  the class for keys (K)
     * @param _classV  the class for values (V)
     */
    public LinHashMap (Class <K> _classK, Class <V> _classV)
    {
        classK = _classK;
        classV = _classV;
        mod1   = 4;                                                          // initial size
        mod2   = 2 * mod1;
        hTable = new ArrayList <> ();
        for (int i = 0; i < mod1; i++) hTable.add (new Bucket ());
    } // constructor

    /********************************************************************************
     * Return a set containing all the entries as pairs of keys and values.
     * @return  the set view of the map
     */
    public Set <Map.Entry <K, V>> entrySet ()
    {
        Set <Map.Entry <K, V>> enSet = new HashSet <Map.Entry <K, V>> ();

        //  T O   B E   I M P L E M E N T E D
        //implemented by jane
        for(int i = 0 ; i < hTable.size() ; i ++) {
            for(Bucket buc = hTable.get(i) ; buc != null;  buc = buc.next) {

                for(int j = 0 ; j < SLOTS ; j ++) {
                    if(buc.key[j] != null) {
                        enSet.add(new AbstractMap.SimpleEntry <K, V> (buc.key[j] , buc.value[j]));
                    }
                }
            }
        }
            
        return enSet;
    } // entrySet

    /********************************************************************************
     * Given the key, look up the value in the hash table.
     * @param key  the key used for look up
     * @return  the value associated with the key
     */
    @SuppressWarnings("unchecked")
    public V get (Object key)
    {
        int i = h (key);
        return find ((K) key, hTable.get (i), true);
    } // get

    /********************************************************************************
     * Put the key-value pair in the hash table.  Split the 'isplit' bucket chain
     * when the load factor is exceeded.
     * @param key    the key to insert
     * @param value  the value to insert
     * @return  the old/previous value, null if none
     */
    public V put (K key, V value)
    {
        int i    = h (key);                                                  // hash to i-th bucket chain
        LinHashMap<K,V>.Bucket bh   = hTable.get (i);                                           // start with home bucket
        V oldV = find (key, bh, false);                                    // find old value associated with key
        out.println ("LinearHashMap.put: key = " + key + ", h() = " + i + ", value = " + value);

        keyCount++;                                                          // increment the key count
        double lf = loadFactor ();                                              // compute the load factor
        if (DEBUG) out.println ("put: load factor = " + lf);
        if (lf > THRESHOLD) split (key,value);                                        // split beyond THRESHOLD

        LinHashMap<K,V>.Bucket b = bh;
        while (true)  {
            if (b.nKeys < SLOTS) { b.add (key, value); return oldV; }
            if (b.next != null) b = b.next; else break;
        } // while

        LinHashMap<K,V>.Bucket bn = new Bucket ();
        bn.add (key, value);
        b.next = bn;                                                         // add new bucket at end of chain
        return oldV;
    } // put

    /********************************************************************************
     * Print the hash table.
     */
    public void print ()
    {
        out.println ("LinHashMap");
        out.println ("-------------------------------------------");

        for (int i = 0; i < hTable.size (); i++) {
            out.print ("Bucket [ " + i + " ] = ");
            int j = 0;
            for (LinHashMap<K,V>.Bucket b = hTable.get (i); b != null; b = b.next) {
                if (j > 0) out.print (" \t\t --> ");
                b.print ();
                j++;
            } // for
        } // for

        out.println ("-------------------------------------------");
    } // print

    /**
     * @author jane odum
     * This method inserts the key in the hTable
     * @param key
     * @param value
     */
    public void insert(K key, V value)
    {

        int i = h(key);
        if (i < isplit) {
            i = h2(key);
        }

        for(Bucket buc = hTable.get(i) ; buc != null;  buc = buc.next) {

            int count =0;

            if(buc.nKeys < SLOTS){

                buc.key[buc.nKeys] = key;
                buc.value[buc.nKeys] = value;
                buc.nKeys++;
                break;

            }
            //this case handles overloading
            else if (buc.nKeys == SLOTS && buc.next == null) {

                buc.next = new Bucket();
                buc.next.key[buc.next.nKeys] = key;
                buc.next.value[buc.next.nKeys] = value;
                buc.next.nKeys++;

                break;
            }


        }
    }

    /********************************************************************************
     * Return the size (SLOTS * number of home buckets) of the hash table. 
     * @return  the size of the hash table
     */
    public int size ()
    {
        return SLOTS * (mod1 + isplit);
    } // size

    /********************************************************************************
     * Split bucket chain 'isplit' by creating a new bucket chain at the end of the
     * hash table and redistributing the keys according to the high resolution hash
     * function 'h2'.  Increment 'isplit'.  If current split phase is complete,
     * reset 'isplit' to zero, and update the hash functions.
     */
    private void split (K key, V value)
    {
        out.println ("split: bucket chain " + isplit);

        //  T O   B E   I M P L E M E N T E D
        int firstBucket = isplit;

        if(isplit+1 == mod1) {

            mod1 = mod1 * 2;
            mod2 = 2 * mod1;
            isplit = 0;

        }
        else {
            isplit++;
        }

        Bucket buc = new Bucket();
        hTable.add(buc);
        //Getting all the values of split bucket and the one we want to add in a map
        Map<K,V> splitBucket = new HashMap<K, V>();
        splitBucket.put(key, value);
        for(Bucket bucket = hTable.get(firstBucket) ; bucket != null;  bucket = bucket.next) {
            for(int i = 0 ; i < SLOTS ; i++) {
                if(bucket.key[i] != null) {
                    splitBucket.put(bucket.key[i], bucket.value[i]);
                }
            }
        }

        //empty the bucket
        hTable.remove(firstBucket);
        hTable.add(firstBucket, new Bucket());

        for(K  ky : splitBucket.keySet()) {
            insert(ky, splitBucket.get(ky));
        }

    } // split

    /********************************************************************************
     * Return the load factor for the hash table.
     * @return  the load factor
     */
    private double loadFactor ()
    {
        return keyCount / (double) size ();
    } // loadFactor

    /********************************************************************************
     * Find the key in the bucket chain that starts with home bucket bh.
     * @param key     the key to find
     * @param bh      the given home bucket
     * @param by_get  whether 'find' is called from 'get' (performance monitored)
     * @return  the current value stored stored for the key
     */
    private V find (K key, Bucket bh, boolean by_get)
    {
        for (LinHashMap<K,V>.Bucket b = bh; b != null; b = b.next) {
            if (by_get) count++;
            V result = b.find (key);
            if (result != null) return result;
        } // for
        return null;
    } // find

    /********************************************************************************
     * Hash the key using the low resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h (Object key)
    {
        return key.hashCode () % mod1;
    } // h

    /********************************************************************************
     * Hash the key using the high resolution hash function.
     * @param key  the key to hash
     * @return  the location of the bucket chain containing the key-value pair
     */
    private int h2 (Object key)
    {
        return key.hashCode () % mod2;
    } // h2

    /********************************************************************************
     * The main method used for testing.
     * @param  args command-line arguments (args [0] gives number of keys to insert)
     */
    public static void main (String [] args)
    {
        int totalKeys = 40;
        boolean RANDOMLY  = false;

        LinHashMap <Integer, Integer> ht = new LinHashMap <> (Integer.class, Integer.class);
        if (args.length == 1) totalKeys = Integer.valueOf (args [0]);

        if (RANDOMLY) {
            var rng = new Random ();
            for (int i = 1; i <= totalKeys; i += 2) ht.put (rng.nextInt (2 * totalKeys), i * i);
        } else {
            for (int i = 1; i <= totalKeys; i += 2) ht.put (i, i * i);
        } // if

        ht.print ();
        for (int i = 0; i <= totalKeys; i++) {
            out.println ("key = " + i + " value = " + ht.get (i));
        } // for
        out.println ("-------------------------------------------");
        out.println ("Average number of buckets accessed = " + ht.count / (double) totalKeys);
    } // main

} // LinHashMap class

