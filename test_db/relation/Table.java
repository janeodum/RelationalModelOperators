
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.lang.Boolean.*;
import static java.lang.System.arraycopy;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names,
 * domains
 * and a list of tuples. Five basic relational algebra operators are provided:
 * project,
 * select, union, minus and join. The insert data manipulation operator is also
 * provided.
 * Missing are update and delete data manipulation operators.
 */
public class Table
        implements Serializable {
    /**
     * Relative path for storage directory
     */
    private static final String DIR = "store" + File.separator;

    /**
     * Filename extension for database files
     */
    private static final String EXT = ".dbf";

    /**
     * Counter for naming temporary tables.
     */
    private static int count = 0;

    /**
     * Table name.
     */
    private String name = null;

    /**
     * Array of attribute names.
     */
    private final String[] attribute;

    /**
     * Array of attribute domains: a domain may be
     * integer types: Long, Integer, Short, Byte
     * real types: Double, Float
     * string types: Character, String
     */
    private final Class[] domain;

    /**
     * Collection of tuples (data storage).
     */
    private final List<Comparable[]> tuples;

    /**
     * Primary key.
     */
    private final String[] key;

    /**
     * Flag to check if the attribute is present in the table.
     */
    private static boolean flag;

    /**
     * Index into tuples (maps key to tuple number).
     */
    private final Map<KeyType, Comparable[]> index;

    /**
     * The supported map types.
     */
    private enum MapType {
        NO_MAP, TREE_MAP, LINHASH_MAP, BPTREE_MAP
    }

    /**
     * The map type to be used for indices. Change as needed.
     */
    private static final MapType mType = MapType.NO_MAP;

    private Class[] domainsTable1s;

    /************************************************************************************
     * Make a map (index) given the MapType.
     */
    private static Map<KeyType, Comparable[]> makeMap() {
        return switch (mType) {
            case TREE_MAP -> new TreeMap<>();
            case LINHASH_MAP -> new LinHashMap<>(KeyType.class, Comparable[].class);
            // case BPTREE_MAP -> new BpTreeMap <> (KeyType.class, Comparable [].class);
            default -> null;
        }; // switch
    } // makeMap

    /************************************************************************************
     * Concatenate two arrays of type T to form a new wider array.
     *
     * @see http://stackoverflow.com/questions/80476/how-to-concatenate-two-arrays-in-java
     *
     * @param arr1 the first array
     * @param arr2 the second array
     * @return a wider array containing all the values from arr1 and arr2
     */
    public static <T> T[] concat(T[] arr1, T[] arr2) {
        T[] result = Arrays.copyOf(arr1, arr1.length + arr2.length);
        arraycopy(arr2, 0, result, arr1.length, arr2.length);
        return result;
    } // concat

    // -----------------------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------------------

    /************************************************************************************
     * Construct an empty table from the meta-data specifications.
     *
     * @param _name      the name of the relation
     * @param _attribute the string containing attributes names
     * @param _domain    the string containing attribute domains (data types)
     * @param _key       the primary key
     */
    public Table(String _name, String[] _attribute, Class[] _domain, String[] _key) {
        name = _name;
        attribute = _attribute;
        domain = _domain;
        key = _key;
        tuples = new ArrayList<>();
        index = makeMap();

    } // primary constructor

    /************************************************************************************
     * Construct a table from the meta-data specifications and data in _tuples list.
     *
     * @param _name      the name of the relation
     * @param _attribute the string containing attributes names
     * @param _domain    the string containing attribute domains (data types)
     * @param _key       the primary key
     * @param _tuples    the list of tuples containing the data
     */
    public Table(String _name, String[] _attribute, Class[] _domain, String[] _key,
            List<Comparable[]> _tuples) {
        name = _name;
        attribute = _attribute;
        domain = _domain;
        key = _key;
        tuples = _tuples;
        index = makeMap();
    } // constructor

    /************************************************************************************
     * Construct an empty table from the raw string specifications.
     *
     * @param _name      the name of the relation
     * @param attributes the string containing attributes names
     * @param domains    the string containing attribute domains (data types)
     * @param _key       the primary key
     */
    public Table(String _name, String attributes, String domains, String _key) {
        this(_name, attributes.split(" "), findClass(domains.split(" ")), _key.split(" "));

        out.println("DDL> create table " + name + " (" + attributes + ")");
    } // constructor

    // ----------------------------------------------------------------------------------
    // Public Methods
    // ----------------------------------------------------------------------------------

    /************************************************************************************
     * Project the tuples onto a lower dimension by keeping only the given
     * attributes.
     * Check whether the original key is included in the projection.
     *
     * #usage movie.project ("title year studioNo")
     *
     * @param attributes the attributes to project onto
     * @return a table of projected tuples
     */
    public Table project(String attributes) {
        out.println("RA> " + name + ".project (" + attributes + ")");
        var attrs = attributes.split(" ");
        var colDomain = extractDom(match(attrs), domain);
        var newKey = (Arrays.asList(attrs).containsAll(Arrays.asList(key))) ? key : attrs;

        List<Comparable[]> rows = new ArrayList<>();

        // T O B E I M P L E M E N T E D
        /**
         * @author jane odum
         *         // If flag is true meaning the attribute is present in the table only
         *         then iterate through all the tuples
         *         // and retrieve the values.
         */
        if (flag) {
            this.tuples.stream().forEach(item -> rows.add(extract(temp, attrs)));
        }

        return new Table(name + count++, attrs, colDomain, newKey, rows);
    } // project

    /************************************************************************************
     * Select the tuples satisfying the given predicate (Boolean function).
     *
     * #usage movie.select (t -> t[movie.col("year")].equals (1977))
     *
     * @param predicate the check condition for tuples
     * @return a table with tuples satisfying the predicate
     */
    public Table select(Predicate<Comparable[]> predicate) {
        out.println("RA> " + name + ".select (" + predicate + ")");

        return new Table(name + count++, attribute, domain, key,
                tuples.stream().filter(t -> predicate.test(t))
                        .collect(Collectors.toList()));
    } // select

    /************************************************************************************
     * Select the tuples satisfying the given key predicate (key = value). Use an
     * index
     * (Map) to retrieve the tuple with the given key value.
     *
     * @param keyVal the given key value
     * @return a table with the tuple satisfying the key predicate
     */
    public Table select(KeyType keyVal) {
        out.println("RA> " + name + ".select (" + keyVal + ")");

        List<Comparable[]> rows = new ArrayList<>();

        // T O B E I M P L E M E N T E D
        // @author Jane odum
        Comparable[] temp = null;
        if (mType == MapType.NO_MAP) {
            out.println("please select a map");
        } else {
            temp = index.get(keyVal);
        }
        if (temp != null) {
            rows.add(temp);
        } // if

        return new Table(name + count++, attribute, domain, key, rows);
    } // select

    /************************************************************************************
     * Union this table and table2. Check that the two tables are compatible.
     *
     * #usage movie.union (show)
     *
     * @param table2 the rhs table in the union operation
     * @return a table representing the union
     */
    public Table union(Table table2) {
        out.println("RA> " + name + ".union (" + table2.name + ")");
        if (!compatible(table2))
            return null;

        List<Comparable[]> rows = new ArrayList<>();

        // T O B E I M P L E M E N T E D

        // Adds all rows from first table
        for (Comparable[] temp : this.tuples) {
            rows.add(temp);
        } // for

        // Adds all rows from second table
        for (Comparable[] temp : table2.tuples) {
            boolean dup = true;
            // checks for duplicate in the table
            for (Comparable[] temp2 : this.tuples) {
                if (temp.equals(temp2)) {
                    dup = false;
                    break;
                }

            }
        }
        if (dup) {
            // tuple is added once there is no duplicate
            rows.add(temp);
        }

        return new Table(name + count++, attribute, domain, key, rows);
    } // union

    /************************************************************************************
     * Take the difference of this table and table2. Check that the two tables are
     * compatible.
     *
     * #usage movie.minus (show)
     *
     * @param table2 The rhs table in the minus operation
     * @return a table representing the difference
     */
    public Table minus(Table table2) {
        out.println("RA> " + name + ".minus (" + table2.name + ")");
        if (!compatible(table2))
            return null;

        List<Comparable[]> rows = new ArrayList<>();

        /*
         * Selects elements from table 1 and table 2, returns rows from table 1
         * but not in table 2
         */
        // Loop iterating through and evaluating if key is in table 2 then not added
        this.tuples.stream().filter(item -> !(table2.tuples.contains(item)))
                .forEach(item -> rows.add(item));

        return new Table(name + count++, attribute, domain, key, rows);
    } // minus

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Tuples from both tables
     * are compared requiring attributes1 to equal attributes2.  Disambiguate attribute
     * names by append "2" to the end of any duplicate attribute name.  Implement using
     * a Nested Loop Join algorithm.
     *
     * #usage movie.join ("studioNo", "name", studio)
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table join (String attributes1, String attributes2, Table table2)
    {
        out.println ("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", "
                                               + table2.name + ")");

        String [] t_attrs = attributes1.split (" ");
        String [] u_attrs = attributes2.split (" ");
        List <Comparable> rows    = new ArrayList <> ();
        
        int [] t_attrsPos = this.match(t_attrs);
        int [] u_attrsPos = table2.match(u_attrs);

        // get the domain of the two tables to join so that we compare before performing join operation
        //in order to save cost
        final Class [] table1Domain = this.extractDom(t_attrsPos, this.domain);
        final Class [] table2Domain = this.extractDom(u_attrsPos, table2.domain);
        String [] table2Attributes = new String [table2.attribute.length];

        if(table1Domain.equals(table2Domain))
        {

       
	
	        //All tables 
            for(Comparable [] a1: this.tuples)
            {
                for(Comparable [] a2 : table2.tuples)
                {
                    int thereIsMatch = 0;
                    for(int j = 0; j < t_attrsPos.length; j++)
                    {
                        if(a1[(int) t_attrsPos[j]].equals(a2[(int)u_attrsPos[j]]))
                        {
                            thereIsMatch++;
                        }
                        if(j == t_attrsPos.length -1 && thereIsMatch == t_attrsPos.length){
                            rows.add(ArrayUtil.concat(a1, a2));
                        }
                    
                    }
                }
            }

            // for the purpsoe of disambiquation append 2 to duplicate attribute
            for (int j = 0; j < table2.attribute.length; j ++) {
                table2Attributes[j] = table2.attribute[j];
            }

            for (int attribute2 = 0; attribute2 < table2Attributes.length; attribute2++) {
                for (int attribute1 = 0; attribute1 < this.attribute.length; attribute1++) {
                    if (this.attribute[attribute1].equalsIgnoreCase(table2Attributes[attribute2])) {
                        table2Attributes[attribute2] = table2Attributes[attribute2] + "2";
                    }
                }
            }
            
        
            
        return new Table (name + count++, concat (attribute, table2Attributes),
                                          concat (domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join".  Same as above, but implemented
     * using an Index Join algorithm.
     *
     * @param attribute1  the attributes of this table to be compared (Foreign Key)
     * @param attribute2  the attributes of table2 to be compared (Primary Key)
     * @param table2      the rhs table in the join operation
     * @return  a table with tuples satisfying the equality predicate
     */
    public Table i_join (String attributes1, String attributes2, Table table2)
    {
        List <Comparable []> rows = new ArrayList <> ();

        if(mType != MapType.TREE_MAP)
        {
            String [] t_attrs = attributes1.split (" ");
            String [] u_attrs = attributes2.split (" ");

            String [] table2Attributes = new String [table2.attribute.length];

            if (u_attrs.length == table2.key.length && 0 == new KeyType(table2.key).compareTo(new KeyType(u_attrs)))
            {
                int [] t_attrsPos = this.match(t_attrs);
                

                for (int i = 0; i < this.tuples.size(); i++)
                {
                    Comparable [] firstData = new Comparable[t_attrsPos.length];
                    for ( int k = 0; k < firstData.length; k++)
                    {
                        firstData[k] = this.tuples.get(i)[t_attrsPos[k]];

                    }//for
                    //Concats the rest of the row and append it to the result
                    Comparable [] secondData = table2.index.get(new KeyType(firstData));
                    if (secondData != null)
                    {
                        rows.add(ArrayUtil.concat(this.tuples.get(i), secondData));
                    }//if
                }//for
            }//if
            else
            {
                out.println("Tables cannot be joined");
                return null;
            }

            // for the purpsoe of disambiquation append 2 to duplicate attribute
            for (int j = 0; j < table2.attribute.length; j ++) {
                table2Attributes[j] = table2.attribute[j];
            }

            for (int attribute2 = 0; attribute2 < table2Attributes.length; attribute2++) {
                for (int attribute1 = 0; attribute1 < this.attribute.length; attribute1++) {
                    if (this.attribute[attribute1].equalsIgnoreCase(table2Attributes[attribute2])) {
                        table2Attributes[attribute2] = table2Attributes[attribute2] + "2";
                    }
                }
            }

            Table result = new Table (name + count++, ArrayUtil.concat(attribute, attributesTable2),
ArrayUtil.concat (domain, table2.domain), this.key, rows);
            return result;
                }
        else
        {
            out.println("Please select an index map.");
            Table result = new Table (name + count++, attribute,
            domain, this.key, rows);
            return result;
        }


        
    } // i_join

    /************************************************************************************
     * Join this table and table2 by performing an "equi-join". Same as above, but
     * implemented
     * using a Hash Join algorithm.
     *
     * @param attribute1 the attributes of this table to be compared (Foreign Key)
     * @param attribute2 the attributes of table2 to be compared (Primary Key)
     * @param table2     the rhs table in the join operation
     * @return a table with tuples satisfying the equality predicate
     */
    public Table h_join(String attributes1, String attributes2, Table table2) {

        // D O N O T I M P L E M E N T

        return null;
    } // h_join

    /************************************************************************************
     * Join this table and table2 by performing an "natural join". Tuples from both
     * tables
     * are compared requiring common attributes to be equal. The duplicate column is
     * also
     * eliminated.
     *
     * #usage movieStar.join (starsIn)
     *
     * @param table2 the rhs table in the join operation
     * @return a table with tuples satisfying the equality predicate
     */
    public Table join(Table table2) {
        out.println("RA> " + name + ".join (" + table2.name + ")");

        var rows = new ArrayList<Comparable[]>();

        // T O B E I M P L E M E N T E D

        // FIX - eliminate duplicate columns
        return new Table(name + count++, concat(attribute, table2.attribute),
                concat(domain, table2.domain), key, rows);
    } // join

    /************************************************************************************
     * Return the column position for the given attribute name.
     *
     * @param attr the given attribute name
     * @return a column position
     */
    public int col(String attr) {
        for (var i = 0; i < attribute.length; i++) {
            if (attr.equals(attribute[i]))
                return i;
        } // for

        return -1; // not found
    } // col

    /************************************************************************************
     * Insert a tuple to the table.
     *
     * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
     *
     * @param tup the array of attribute values forming the tuple
     * @return whether insertion was successful
     */
    public boolean insert(Comparable[] tup) {
        out.println("DML> insert into " + name + " values ( " + Arrays.toString(tup) + " )");

        if (typeCheck(tup)) {
            tuples.add(tup);
            var keyVal = new Comparable[key.length];
            var cols = match(key);
            for (var j = 0; j < keyVal.length; j++)
                keyVal[j] = tup[cols[j]];
            if (mType != MapType.NO_MAP)
                index.put(new KeyType(keyVal), tup);
            return true;
        } else {
            return false;
        } // if
    } // insert

    /************************************************************************************
     * Get the name of the table.
     *
     * @return the table's name
     */
    public String getName() {
        return name;
    } // getName

    /************************************************************************************
     * Print this table.
     */
    public void print() {
        out.println("\n Table " + name);
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
        out.print("| ");
        for (var a : attribute)
            out.printf("%15s", a);
        out.println(" |");
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
        for (var tup : tuples) {
            out.print("| ");
            for (var attr : tup)
                out.printf("%15s", attr);
            out.println(" |");
        } // for
        out.print("|-");
        out.print("---------------".repeat(attribute.length));
        out.println("-|");
    } // print

    /************************************************************************************
     * Print this table's index (Map).
     */
    public void printIndex() {
        out.println("\n Index for " + name);
        out.println("-------------------");
        if (mType != MapType.NO_MAP) {
            for (var e : index.entrySet()) {
                out.println(e.getKey() + " -> " + Arrays.toString(e.getValue()));
            } // for
        } // if
        out.println("-------------------");
    } // printIndex

    /************************************************************************************
     * Load the table with the given name into memory.
     *
     * @param name the name of the table to load
     */
    public static Table load(String name) {
        Table tab = null;
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(DIR + name + EXT));
            tab = (Table) ois.readObject();
            ois.close();
        } catch (IOException ex) {
            out.println("load: IO Exception");
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            out.println("load: Class Not Found Exception");
            ex.printStackTrace();
        } // try
        return tab;
    } // load

    /************************************************************************************
     * Save this table in a file.
     */
    public void save() {
        try {
            var oos = new ObjectOutputStream(new FileOutputStream(DIR + name + EXT));
            oos.writeObject(this);
            oos.close();
        } catch (IOException ex) {
            out.println("save: IO Exception");
            ex.printStackTrace();
        } // try
    } // save

    // ----------------------------------------------------------------------------------
    // Private Methods
    // ----------------------------------------------------------------------------------

    /************************************************************************************
     * Determine whether the two tables (this and table2) are compatible, i.e., have
     * the same number of attributes each with the same corresponding domain.
     *
     * @param table2 the rhs table
     * @return whether the two tables are compatible
     */
    private boolean compatible(Table table2) {
        if (domain.length != table2.domain.length) {
            out.println("compatible ERROR: table have different arity");
            return false;
        } // if
        for (var j = 0; j < domain.length; j++) {
            if (domain[j] != table2.domain[j]) {
                out.println("compatible ERROR: tables disagree on domain " + j);
                return false;
            } // if
        } // for
        return true;
    } // compatible

    /************************************************************************************
     * Match the column and attribute names to determine the domains.
     *
     * @param column the array of column names
     * @return an array of column index positions
     */
    private int[] match(String[] column) {
        int[] colPos = new int[column.length];

        for (var j = 0; j < column.length; j++) {
            var matched = false;
            for (var k = 0; k < attribute.length; k++) {
                if (column[j].equals(attribute[k])) {
                    matched = true;
                    colPos[j] = k;
                } // for
            } // for
            if (!matched) {
                out.println("match: domain not found for " + column[j]);
            } // if
        } // for

        return colPos;
    } // match

    /************************************************************************************
     * Extract the attributes specified by the column array from tuple t.
     *
     * @param t      the tuple to extract from
     * @param column the array of column names
     * @return a smaller tuple extracted from tuple t
     */
    private Comparable[] extract(Comparable[] t, String[] column) {
        var tup = new Comparable[column.length];
        var colPos = match(column);
        for (var j = 0; j < column.length; j++)
            tup[j] = t[colPos[j]];
        return tup;
    } // extract

    /************************************************************************************
     * Check the size of the tuple (number of elements in list) as well as the type
     * of
     * each value to ensure it is from the right domain.
     *
     * @param t the tuple as a list of attribute values
     * @return whether the tuple has the right size and values that comply
     *         with the given domains
     */
    private boolean typeCheck(Comparable[] t) {
        // T O B E I M P L E M E N T E D

        return true;
    } // typeCheck

    /************************************************************************************
     * Find the classes in the "java.lang" package with given names.
     *
     * @param className the array of class name (e.g., {"Integer", "String"})
     * @return an array of Java classes
     */
    private static Class[] findClass(String[] className) {
        var classArray = new Class[className.length];

        for (var i = 0; i < className.length; i++) {
            try {
                classArray[i] = Class.forName("java.lang." + className[i]);
            } catch (ClassNotFoundException ex) {
                out.println("findClass: " + ex);
            } // try
        } // for

        return classArray;
    } // findClass

    /************************************************************************************
     * Extract the corresponding domains.
     *
     * @param colPos the column positions to extract.
     * @param group  where to extract from
     * @return the extracted domains
     */
    private Class[] extractDom(int[] colPos, Class[] group) {
        var obj = new Class[colPos.length];

        for (var j = 0; j < colPos.length; j++) {
            obj[j] = group[colPos[j]];
        } // for

        return obj;
    } // extractDom

} // Table class
