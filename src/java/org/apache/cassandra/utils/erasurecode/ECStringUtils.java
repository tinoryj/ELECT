/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
