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
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class utils {
    private static final Logger logger = LoggerFactory.getLogger(utils.class);


    // public static class ByteObjectConversion<T> {

    //     // convert object to byte array
    //     public byte[] toByteArray(T object) throws Exception {

    //         ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //         ObjectOutputStream oos;

    //         try {
    //             logger.debug("rymDebug: start to transform, obj is {}, class is {}", object,object.getClass());
    //             oos = new ObjectOutputStream(baos);
    //             oos.writeObject(object);
    //             byte[] bytes = baos.toByteArray();
    //             logger.debug("rymDebug: get bytes is {}", bytes);
    //             return bytes;
    //         } catch (IOException e) {
    //             // TODO Auto-generated catch block
    //             logger.error("rymError: cannot serialize this fucking obj! error info {}", e);
    //         }
    //         return null;
    //     }
    
    //     //convert byte array to object
    //     public T fromByteArray(byte[] bytes) throws Exception {
    //         try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    //              ObjectInputStream ois = new ObjectInputStream(bis)) {
    //             return (T) ois.readObject();
    //         }
    //     }

    public static class ByteObjectConversion {
        public static byte[] objectToByteArray(Serializable obj) throws Exception {
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

}
