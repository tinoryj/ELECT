/*
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

package org.apache.cassandra.io.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErasureCodeTest {
    private static Logger logger = LoggerFactory.getLogger(ErasureCodeTest.class.getName());

    public static void erasureCodeTest() throws IOException {
        final int k = 4, m = 2;
        int codeLength = 1024;
        Random random = new Random((long) 123);

        // Generate encoder and decoder
        ErasureCoderOptions ecOptions = new ErasureCoderOptions(k, m);
        ErasureEncoder encoder = new NativeRSEncoder(ecOptions);
        ErasureDecoder decoder = new NativeRSDecoder(ecOptions);

        // Encoding input and output
        ByteBuffer[] data = new ByteBuffer[k];
        ByteBuffer[] parity = new ByteBuffer[m];
        ByteBuffer[] SecondParity = new ByteBuffer[m];

        // Decoding input and output
        ByteBuffer[] recoverySrc = new ByteBuffer[k];
        int[] eraseIndexes = { 0 };
        int[] decodeIndexes = { 4, 1, 2, 3 };
        ByteBuffer[] outputs = new ByteBuffer[1];

        // Prepare recoverySrc for encoding
        byte[] tmpArray = new byte[codeLength];
        for (int i = 0; i < k; i++) {
            data[i] = ByteBuffer.allocateDirect(codeLength);
            random.nextBytes(tmpArray);
            data[i].put(tmpArray);
            data[i].rewind();
        }
        // Prepare outputs for encoding
        for (int i = 0; i < m; i++) {
            parity[i] = ByteBuffer.allocateDirect(codeLength);
            SecondParity[i] = ByteBuffer.allocateDirect(codeLength);
        }

        // Encode
        logger.debug("ErasureCodeTest - first encode()!");
        encoder.encode(data, parity);

        // Prepare recoverySrc for decoding
        recoverySrc[0] = parity[0];
        for (int i = 1; i < k; i++) {
            data[i].rewind();
            recoverySrc[i] = data[i];
        }

        // Prepare outputs for decoding
        outputs[0] = ByteBuffer.allocateDirect(codeLength);

        // Decode
        logger.debug("ErasureCodeTest - first decode()!");
        decoder.decode(recoverySrc, decodeIndexes, eraseIndexes, outputs);

        data[0].rewind();
        if (outputs[0].compareTo(data[0]) == 0) {
            logger.debug("ErasureCodeTest - decoding Succeeded, same recovered data!");
        } else {
            logger.debug("ErasureCodeTest - decoding Failed, diffent recovered data ");
        }

        // update
        logger.debug("ErasureCodeTest - Perform encode update for data block 1!");
        ByteBuffer[] dataUpdate = new ByteBuffer[2 + m];
        dataUpdate[0] = ByteBuffer.allocateDirect(codeLength);
        data[0].rewind();
        dataUpdate[0] = data[0];
        dataUpdate[0].rewind();
        dataUpdate[1] = ByteBuffer.allocateDirect(codeLength);
        random.nextBytes(tmpArray);
        dataUpdate[1].put(tmpArray);
        dataUpdate[1].rewind();
        for (int i = 0; i < m; i++) {
            parity[i].rewind();
            dataUpdate[i + 2] = ByteBuffer.allocateDirect(codeLength);
            dataUpdate[i + 2] = parity[i];
        }

        // check by encode again.
        ByteBuffer[] dataUpdateXOR = new ByteBuffer[k];
        dataUpdateXOR[0] = ByteBuffer.allocateDirect(codeLength);
        // compute XOR for dataUpdate[0] and dataUpdate[1]
        dataUpdate[0].rewind();
        dataUpdate[1].rewind();
        for (int i = 0; i < codeLength; i++) {
            dataUpdateXOR[0].put((byte) (dataUpdate[0].get() ^ dataUpdate[1].get()));
        }
        for (int i = 1; i < k; i++) {
            data[i].rewind();
            dataUpdateXOR[i] = ByteBuffer.allocateDirect(codeLength);
            dataUpdateXOR[i] = data[i];
            dataUpdateXOR[i].rewind();
        }
        encoder.encode(dataUpdateXOR, SecondParity);
        // perform update
        encoder.encodeUpdate(dataUpdate, parity, 1);

        recoverySrc[0] = null;
        for (int i = 1; i < k; i++) {
            data[i].rewind();
            recoverySrc[i] = data[i];
            // logger.debug("recoverySrc[" + i + "]: position() = " +
            // recoverySrc[i].position() + ", remaining() = "
            // + recoverySrc[i].remaining());
        }
        recoverySrc[0] = parity[0];

        // Prepare outputs for decoding
        outputs[0] = ByteBuffer.allocateDirect(codeLength);

        // Decode
        logger.debug("ErasureCodeTest - second decode()!");
        decoder.decode(recoverySrc, decodeIndexes, eraseIndexes, outputs);

        data[0].rewind();
        if (outputs[0].compareTo(dataUpdate[0]) == 0) {
            logger.debug("ErasureCodeTest - decoding Succeeded, same recovered data!");
        } else {
            logger.debug("ErasureCodeTest - decoding Failed, diffent recovered data ");
        }

        encoder.release();
        decoder.release();
    }

    public static void main(String[] args) throws IOException {
        erasureCodeTest();
    }
}
