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

package org.apache.flink.connector.pulsar.sink.committer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** A serializer used to serialize {@link PulsarCommittable}. */
public class PulsarCommittableSerializer implements SimpleVersionedSerializer<PulsarCommittable> {

    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 1;
    public static final String TOPIC_PLACEHOLDER = "Topic_Placeholder";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false);
    }

    @Override
    public int getVersion() {
        return VERSION_V2;
    }

    @Override
    public byte[] serialize(PulsarCommittable obj) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(obj);
    }

    @Override
    public PulsarCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_V1) {
            return deserializeV1(serialized);
        }
        PulsarCommittablePojo pulsarCommittablePojo = OBJECT_MAPPER.readValue(serialized, PulsarCommittablePojo.class);
        TxnID txnID = new TxnID(pulsarCommittablePojo.txnID.mostSigBits, pulsarCommittablePojo.txnID.leastSigBits);
        return new PulsarCommittable(txnID, pulsarCommittablePojo.latestPublishedMessages);
    }

    public PulsarCommittable deserializeV1(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            String topic = in.readUTF();
            return new PulsarCommittable(txnID, topic);
        }
    }

    public PulsarCommittable deserializeV2(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            TxnID txnID = new TxnID(mostSigBits, leastSigBits);
            String topic = in.readUTF();
            return new PulsarCommittable(txnID, topic);
        }
    }

    private static class PulsarCommittablePojo {
        private TxnIdPojo txnID;
        private Map<String, MessageIdPojo> latestPublishedMessages;

        public PulsarCommittablePojo() {}

        public TxnIdPojo getTxnID() {
            return txnID;
        }

        public void setTxnID(TxnIdPojo txnID) {
            this.txnID = txnID;
        }

        public Map<String, MessageIdPojo> getLatestPublishedMessages() {
            return latestPublishedMessages;
        }

        public void setLatestPublishedMessages(
                Map<String, MessageIdPojo> latestPublishedMessages) {
            this.latestPublishedMessages = latestPublishedMessages;
        }
    }

    private static class TxnIdPojo {
        private long mostSigBits;
        private long leastSigBits;

        public TxnIdPojo() {}

        public long getMostSigBits() {
            return mostSigBits;
        }

        public void setMostSigBits(long mostSigBits) {
            this.mostSigBits = mostSigBits;
        }

        public long getLeastSigBits() {
            return leastSigBits;
        }

        public void setLeastSigBits(long leastSigBits) {
            this.leastSigBits = leastSigBits;
        }
    }
}
