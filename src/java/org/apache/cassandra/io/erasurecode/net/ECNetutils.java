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

package org.apache.cassandra.io.erasurecode.net;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.Index.Indexer;
import org.apache.cassandra.io.erasurecode.net.ECSyncSSTable.SSTablesInBytes;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ECNetutils {
    private static final Logger logger = LoggerFactory.getLogger(ECNetutils.class);
    
    private static final String dataForRewriteDir = System.getProperty("user.dir")+"/data/tmp/";
    private static final String receivedParityCodeDir = System.getProperty("user.dir")+"/data/receivedParityHashes/";
    private static final String dataDir = System.getProperty("user.dir")+"/data/data/";
    private static final String localParityCodeDir = System.getProperty("user.dir")+"/data/localParityHashes/";

    public static class ByteObjectConversion {
        public static byte[] objectToByteArray(Serializable obj) throws IOException {
            logger.debug("rymDebug: start to transform");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();
            return bos.toByteArray();
        }

        public static Object byteArrayToObject(byte[] bytes) throws Exception {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object obj = ois.readObject();
            bis.close();
            ois.close();
            return obj;
        }
    }

    public static class StripIDToSSTHashAndParityNodes implements Serializable {
        public final String stripID;
        public final String sstHash;
        public final List<InetAddressAndPort> parityNodes;
        public StripIDToSSTHashAndParityNodes(String stripID, String sstHash, List<InetAddressAndPort> parityNodes) {
            this.stripID = stripID;
            this.sstHash = sstHash;
            this.parityNodes = parityNodes;
        }
    }

    public static class SSTablesInBytesConverter {

        public static byte[] toByteArray(SSTablesInBytes sstables) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(sstables);
            oos.flush();
            return baos.toByteArray();
        }
    
        public static SSTablesInBytes fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (SSTablesInBytes) ois.readObject();
        }
    }

    public static class DecoratedKeyComparator implements Comparator<DecoratedKey>
    {
        public int compare(DecoratedKey o1, DecoratedKey o2)
        {
            return o2.compareTo(o1);
        }
    };

    public static String getDataForRewriteDir() {
        return dataForRewriteDir;
    }

    public static String getReceivedParityCodeDir() {
        return receivedParityCodeDir;
    }

    public static String getDataDir() {
        return dataDir;
    }

    public static String getLocalParityCodeDir() {
        return localParityCodeDir;
    }

    public static byte[] readBytesFromFile(String fileName) throws IOException
    {
        // String fileName = descriptor.filenameFor(Component.DATA);
        File file = new File(fileName);
        long fileLength = file.length();
        FileInputStream fileStream = new FileInputStream(fileName);
        byte[] buffer = new byte[(int)fileLength];
        int offset = 0;
        int numRead = 0;
        while (offset < buffer.length && (numRead = fileStream.read(buffer, offset, buffer.length - offset)) >= 0) {
            offset += numRead;
        }
        if (offset != buffer.length) {
            throw new IOException(String.format("Could not read %s, only read %d bytes", fileName, offset));
        }
        fileStream.close();
        return buffer;
        // return ByteBuffer.wrap(buffer);
    }

    public static void writeBytesToFile(String fileName, byte[] buffer) throws IOException
    {
        try (FileOutputStream outputStream = new FileOutputStream(fileName)) {
            outputStream.write(buffer);
        } catch (Exception e) {
            logger.error("rymERROR: failed to write bytes to file, {}", e);
        }
    }


    public static Optional<Path> findDirectoryByPrefix(Path parentDirectory, String prefix) throws IOException {
        return Files.list(parentDirectory)
                .filter(path -> Files.isDirectory(path))
                .filter(path -> path.getFileName().toString().startsWith(prefix))
                .findFirst();
    }


    public static String stringToHex(String str) {
        byte[] bytes = str.getBytes();
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16))
                    .append(Character.forDigit((b & 0xF), 16));
        }
        return hex.toString();
    }


    public static void main(String[] args) throws IOException {
        // SSTableMetadataViewer metawriter = new SSTableMetadataViewer(false, false, Integer.MAX_VALUE, TimeUnit.MICROSECONDS, System.out);
        
        // String fname = System.getProperty("user.dir")+"/data/data/nb-1712-big-Statistics.db";
        // File sstable = new File(fname);
        // logger.info("absolutePath is {}, path is {}, parentPath is {}, parent is {}",
        //  sstable.absolutePath(), sstable.path(), sstable.parentPath(),sstable.parent());
        // Descriptor desc = Descriptor.fromFilename(fname);
        // logger.info("read from {}, desc is {}, id is {}, version is {}, format type is {}, TOC file name is {}",
        //  fname, desc, desc.id, desc.version, desc.formatType, desc.filenameFor(Component.TOC));


        // read Statistics.db
        // EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);

        // Map<MetadataType, MetadataComponent> sstableMetadata;
        // try {
        //     sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        //     logger.info("statsmetadata is {}", sstableMetadata.toString());
        // } catch (Throwable t) {
        //     throw new CorruptSSTableException(t, desc.filenameFor(Component.STATS));
        // }


        // try {
        //     byte[] data = Files.readAllBytes(Paths.get(fname));
        //     logger.info("data length is {}, content is {}", data.length, data);
        // } catch (IOException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }

        byte[] bytes = {0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02};


        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInput dataIn = new DataInputStream(in);

        int value1 = dataIn.readInt();
        int value2 = dataIn.readInt();
        
        logger.debug(" read value1 {}, value2 {}", value1, value2);


    }


}
