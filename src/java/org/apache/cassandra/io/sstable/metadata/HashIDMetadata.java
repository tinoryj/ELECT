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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Compaction related SSTable metadata.
 *
 * Only loaded for <b>compacting</b> SSTables at the time of compaction.
 */
public class HashIDMetadata extends MetadataComponent {
    public static final IMetadataComponentSerializer serializer = new HashIDMetadataSerializer();

    public final String hashID;

    public HashIDMetadata(String hashID) {
        this.hashID = hashID;
    }

    public MetadataType getType() {
        return MetadataType.COMPACTION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        // keeping equals and hashCode as all classes inheriting from MetadataComponent
        // implement them but we have really nothing to compare
        return true;
    }

    @Override
    public int hashCode() {
        // see comment in equals
        return 32;
    }

    public static class HashIDMetadataSerializer implements IMetadataComponentSerializer<HashIDMetadata> {
        public int serializedSize(Version version, HashIDMetadata component) throws IOException {
            int sz = 0;
            byte[] serializedCardinality = component.hashID.getBytes();
            return TypeSizes.sizeof(serializedCardinality.length) + serializedCardinality.length + sz;
        }

        public void serialize(Version version, HashIDMetadata component, DataOutputPlus out) throws IOException {
            ByteArrayUtil.writeWithLength(component.hashID.getBytes(), out);
        }

        public HashIDMetadata deserialize(Version version, DataInputPlus in) throws IOException {
            String hashID = ByteBufferUtil.readBytes(in, 32).toString();
            return new HashIDMetadata(hashID);
        }
    }
}
