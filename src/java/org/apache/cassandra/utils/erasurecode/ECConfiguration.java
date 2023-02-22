package org.apache.cassandra.utils.erasurecode;

import org.apache.cassandra.utils.erasurecode.ECStringUtils;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ECConfiguration {
    // For get class by name
    private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, WeakReference<Class<?>>>>();
    private ClassLoader classLoader;
    {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = ECConfiguration.class.getClassLoader();
        }
    }

    /**
     * A unique class which is used as a sentinel value in the caching
     * for getClassByName. {@link Configuration#getClassByNameOrNull(String)}
     */
    private static abstract class NegativeCacheSentinel {
    }

    /**
     * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
     */
    private static final Class<?> NEGATIVE_CACHE_SENTINEL = NegativeCacheSentinel.class;

    /**
     * Load a class by name.
     * 
     * @param name the class name.
     * @return the class object.
     * @throws ClassNotFoundException if the class is not found.
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        Class<?> ret = getClassByNameOrNull(name);
        if (ret == null) {
            throw new ClassNotFoundException("Class " + name + " not found");
        }
        return ret;
    }

    /**
     * Load a class by name, returning null rather than throwing an exception
     * if it couldn't be loaded. This is to avoid the overhead of creating
     * an exception.
     * 
     * @param name the class name
     * @return the class object, or null if it could not be found.
     */
    public Class<?> getClassByNameOrNull(String name) {
        Map<String, WeakReference<Class<?>>> map;

        synchronized (CACHE_CLASSES) {
            map = CACHE_CLASSES.get(classLoader);
            if (map == null) {
                map = Collections.synchronizedMap(
                        new WeakHashMap<String, WeakReference<Class<?>>>());
                CACHE_CLASSES.put(classLoader, map);
            }
        }

        Class<?> clazz = null;
        WeakReference<Class<?>> ref = map.get(name);
        if (ref != null) {
            clazz = ref.get();
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(name, true, classLoader);
            } catch (ClassNotFoundException e) {
                // Leave a marker that the class isn't found
                map.put(name, new WeakReference<Class<?>>(NEGATIVE_CACHE_SENTINEL));
                return null;
            }
            // two putters can race here, but they'll put the same class
            map.put(name, new WeakReference<Class<?>>(clazz));
            return clazz;
        } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
            return null; // not found
        } else {
            // cache hit
            return clazz;
        }
    }

    // for get string

    /**
     * Get the comma delimited values of the <code>name</code> property as
     * a collection of <code>String</code>s.
     * If no such property is specified then empty collection is returned.
     * <p>
     * This is an optimized version of {@link #getStrings(String)}
     * 
     * @param name property name.
     * @return property value as a collection of <code>String</code>s.
     */
    public Collection<String> getStringCollection(String name) {
        String valueString = get(name);
        return ECStringUtils.getStringCollection(valueString);
    }

    /**
     * Get the comma delimited values of the <code>name</code> property as
     * an array of <code>String</code>s.
     * If no such property is specified then <code>null</code> is returned.
     * 
     * @param name property name.
     * @return property value as an array of <code>String</code>s,
     *         or <code>null</code>.
     */
    public String[] getStrings(String name) {
        String valueString = get(name);
        return ECStringUtils.getStrings(valueString);
    }

    /**
     * Get the comma delimited values of the <code>name</code> property as
     * an array of <code>String</code>s.
     * If no such property is specified then default value is returned.
     * 
     * @param name         property name.
     * @param defaultValue The default value
     * @return property value as an array of <code>String</code>s,
     *         or default value.
     */
    public String[] getStrings(String name, String... defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        } else {
            return ECStringUtils.getStrings(valueString);
        }
    }

    /**
     * Get the value of the <code>name</code> property, <code>null</code> if
     * no such property exists. If the key is deprecated, it returns the value of
     * the first key which replaces the deprecated key and is not null.
     * 
     * Values are processed for <a href="#VariableExpansion">variable expansion</a>
     * before being returned.
     *
     * As a side effect get loads the properties from the sources if called for
     * the first time as a lazy init.
     * 
     * @param name the property name, will be trimmed before get value.
     * @return the value of the <code>name</code> or its replacing property,
     *         or null if no such property exists.
     */
    public String get(String name) {
        String result = null;
        return result;
    }

    /**
     * Get the value of the <code>name</code>. If the key is deprecated,
     * it returns the value of the first key which replaces the deprecated key
     * and is not null.
     * If no such property exists,
     * then <code>defaultValue</code> is returned.
     * 
     * @param name         property name, will be trimmed before get value.
     * @param defaultValue default value.
     * @return property value, or <code>defaultValue</code> if the property
     *         doesn't exist.
     */
    public String get(String name, String defaultValue) {
        String result = null;
        return result;
    }
}
