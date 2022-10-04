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
package org.apache.cassandra.utils.erasurecode.coder;


import org.apache.cassandra.utils.erasurecode.CodecUtil;
import org.apache.cassandra.utils.erasurecode.ECBlock;
import org.apache.cassandra.utils.erasurecode.ECBlockGroup;
import org.apache.cassandra.utils.erasurecode.ErasureCodeConstants;
import org.apache.cassandra.utils.erasurecode.ErasureCoderOptions;
import org.apache.cassandra.utils.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Reed-Solomon erasure encoder that encodes a block group.
 *
 * It implements {@link ErasureCoder}.
 */

public class RSErasureEncoder extends ErasureEncoder {
  private RawErasureEncoder rawEncoder;

  public RSErasureEncoder(ErasureCoderOptions options) {
    super(options);
  }

  @Override
  protected ErasureCodingStep prepareEncodingStep(final ECBlockGroup blockGroup) {

    RawErasureEncoder rawEncoder = checkCreateRSRawEncoder();

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureEncodingStep(inputBlocks,
        getOutputBlocks(blockGroup), rawEncoder);
  }

  private RawErasureEncoder checkCreateRSRawEncoder() {
    if (rawEncoder == null) {
      // TODO: we should create the raw coder according to codec.
      rawEncoder = CodecUtil.createRawEncoder(getConf(),
          ErasureCodeConstants.RS_CODEC_NAME, getOptions());
    }
    return rawEncoder;
  }

  @Override
  public void release() {
    if (rawEncoder != null) {
      rawEncoder.release();
    }
  }

  @Override
  public boolean preferDirectBuffer() {
    return false;
  }
}
