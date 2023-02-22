package org.apache.cassandra.utils.erasurecode;

import java.util.Collection;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

public class ECStringUtils {
    /**
     * Returns an arraylist of strings.
     * 
     * @param str the comma separated string values
     * @return the arraylist of the comma separated string values
     */
    public static String[] getStrings(String str) {
        String delim = ",";
        return getStrings(str, delim);
    }

    /**
     * Returns an arraylist of strings.
     * 
     * @param str   the string values
     * @param delim delimiter to separate the values
     * @return the arraylist of the separated string values
     */
    public static String[] getStrings(String str, String delim) {
        Collection<String> values = getStringCollection(str, delim);
        if (values.size() == 0) {
            return null;
        }
        return values.toArray(new String[values.size()]);
    }

    /**
     * Returns a collection of strings.
     * 
     * @param str comma separated string values
     * @return an <code>ArrayList</code> of string values
     */
    public static Collection<String> getStringCollection(String str) {
        String delim = ",";
        return getStringCollection(str, delim);
    }

    /**
     * Returns a collection of strings.
     * 
     * @param str
     *              String to parse
     * @param delim
     *              delimiter to separate the values
     * @return Collection of parsed elements.
     */
    public static Collection<String> getStringCollection(String str, String delim) {
        List<String> values = new ArrayList<String>();
        if (str == null)
            return values;
        StringTokenizer tokenizer = new StringTokenizer(str, delim);
        while (tokenizer.hasMoreTokens()) {
            values.add(tokenizer.nextToken());
        }
        return values;
    }

}
