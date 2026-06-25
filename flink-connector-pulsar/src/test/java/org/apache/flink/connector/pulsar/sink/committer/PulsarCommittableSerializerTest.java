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

import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for serializing and deserializing {@link PulsarCommittable} with {@link
 * PulsarCommittableSerializer}.
 */
class PulsarCommittableSerializerTest {

    private static final PulsarCommittableSerializer INSTANCE = new PulsarCommittableSerializer();

    @Test
    void versionIsV2() {
        assertThat(INSTANCE.getVersion()).isEqualTo(2);
    }

    @Test
    void v2SerializeAndDeserializeWithMessages() throws IOException {
        TxnID txnID = randomTxnID();
        Map<String, MessageIdPojo> messages = new HashMap<>();
        messages.put("topic-a", new MessageIdPojo(1L, 2L, 3, 0));
        messages.put("topic-b", new MessageIdPojo(10L, 20L, 1, 0));

        PulsarCommittable original = new PulsarCommittable(txnID, messages);
        byte[] bytes = INSTANCE.serialize(original);
        PulsarCommittable deserialized = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getVersion()).isEqualTo(2);
        assertThat(deserialized.getTxnID()).isEqualTo(txnID);
        assertThat(deserialized.getLatestPublishedMessages()).hasSize(2);
        assertThat(deserialized.getLatestPublishedMessages().get("topic-a"))
                .isEqualTo(new MessageIdPojo(1L, 2L, 3, 0));
        assertThat(deserialized.getLatestPublishedMessages().get("topic-b"))
                .isEqualTo(new MessageIdPojo(10L, 20L, 1, 0));
    }

    @Test
    void v2SerializeAndDeserializeWithEmptyMessages() throws IOException {
        TxnID txnID = randomTxnID();
        PulsarCommittable original = new PulsarCommittable(txnID, Collections.emptyMap());
        byte[] bytes = INSTANCE.serialize(original);
        PulsarCommittable deserialized = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getVersion()).isEqualTo(2);
        assertThat(deserialized.getTxnID()).isEqualTo(txnID);
        assertThat(deserialized.getLatestPublishedMessages()).isEmpty();
    }

    @Test
    void v2SerializeIsJson() throws IOException {
        TxnID txnID = randomTxnID();
        PulsarCommittable committable = new PulsarCommittable(txnID, Collections.emptyMap());
        byte[] bytes = INSTANCE.serialize(committable);

        String json = new String(bytes);
        assertThat(json).startsWith("{");
        assertThat(json).contains("\"txnID\"");
        assertThat(json).contains("\"latestPublishedMessages\"");
        assertThat(json).contains("\"mostSigBits\"");
        assertThat(json).contains("\"leastSigBits\"");
        assertThat(json).contains(String.valueOf(txnID.getMostSigBits()));
        assertThat(json).contains(String.valueOf(txnID.getLeastSigBits()));
    }

    @Test
    void v1DeserializationCompat() throws IOException {
        TxnID expectedTxnID = randomTxnID();

        // Simulate V1 binary format: just two longs (mostSigBits, leastSigBits)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(expectedTxnID.getMostSigBits());
        dos.writeLong(expectedTxnID.getLeastSigBits());
        dos.flush();
        byte[] v1Bytes = baos.toByteArray();

        PulsarCommittable deserialized = INSTANCE.deserialize(1, v1Bytes);

        assertThat(deserialized.getVersion()).isEqualTo(1);
        assertThat(deserialized.getTxnID()).isEqualTo(expectedTxnID);
        assertThat(deserialized.getLatestPublishedMessages()).isEmpty();
    }

    @Test
    void v1DeserializationProducesEmptyMessageMap() throws IOException {
        TxnID txnID = randomTxnID();
        byte[] v1Bytes = buildV1Bytes(txnID);

        PulsarCommittable deserialized = INSTANCE.deserialize(1, v1Bytes);

        assertThat(deserialized.getVersion()).isEqualTo(1);
        assertThat(deserialized.getLatestPublishedMessages()).isNotNull();
        assertThat(deserialized.getLatestPublishedMessages()).isEmpty();
    }

    @Test
    void v2AndV1DeliverSameTxnIDForRoundTrip() throws IOException {
        TxnID txnID = randomTxnID();

        // V2 round-trip
        PulsarCommittable v2Original = new PulsarCommittable(txnID, Collections.emptyMap());
        byte[] v2Bytes = INSTANCE.serialize(v2Original);
        PulsarCommittable v2Deserialized = INSTANCE.deserialize(2, v2Bytes);
        assertThat(v2Deserialized.getVersion()).isEqualTo(2);
        assertThat(v2Deserialized.getTxnID()).isEqualTo(txnID);

        // V1 round-trip (old format simulated)
        byte[] v1Bytes = buildV1Bytes(txnID);
        PulsarCommittable v1Deserialized = INSTANCE.deserialize(1, v1Bytes);
        assertThat(v1Deserialized.getVersion()).isEqualTo(1);
        assertThat(v1Deserialized.getTxnID()).isEqualTo(txnID);
    }

    @Test
    void unknownVersionFallsBackToV2Deserializer() throws IOException {
        TxnID txnID = randomTxnID();
        PulsarCommittable original = new PulsarCommittable(txnID);
        byte[] v2Bytes = INSTANCE.serialize(original);

        // Future version 99 should still deserialize via V2 (JSON) path
        PulsarCommittable deserialized = INSTANCE.deserialize(99, v2Bytes);

        assertThat(deserialized.getVersion()).isEqualTo(2);
        assertThat(deserialized.getTxnID()).isEqualTo(txnID);
    }

    @Test
    void v1DeserializationIsByteOrderSafe() throws IOException {
        // Verify that V1 binary deserialization correctly recovers the original TxnID
        // byte-for-byte, excluding leftover trailing bytes from padded streams.
        TxnID txnID = new TxnID(Long.MAX_VALUE, Long.MIN_VALUE);
        byte[] v1Bytes = buildV1Bytes(txnID);

        PulsarCommittable deserialized = INSTANCE.deserialize(1, v1Bytes);

        assertThat(deserialized.getTxnID()).isEqualTo(txnID);
        assertThat(deserialized.getVersion()).isEqualTo(1);
        assertThat(deserialized.getTxnID().getMostSigBits()).isEqualTo(Long.MAX_VALUE);
        assertThat(deserialized.getTxnID().getLeastSigBits()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void v1BytesWithV2VersionThrowsOnDeserialize() throws IOException {
        byte[] v1Bytes = buildV1Bytes(randomTxnID());

        // Feeding binary bytes to V2 JSON parser should fail
        assertThatThrownBy(() -> INSTANCE.deserialize(2, v1Bytes)).isInstanceOf(IOException.class);
    }

    @Test
    void serializeAndDeserializeWithDeprecatedConstructor() throws IOException {
        TxnID txnID = randomTxnID();
        PulsarCommittable original = new PulsarCommittable(txnID);
        byte[] bytes = INSTANCE.serialize(original);
        PulsarCommittable deserialized = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.getVersion()).isEqualTo(2);
        assertThat(deserialized.getTxnID()).isEqualTo(txnID);
        assertThat(deserialized.getLatestPublishedMessages()).isEmpty();
    }

    // --- helpers ---

    private static TxnID randomTxnID() {
        return new TxnID(
                ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());
    }

    private static byte[] buildV1Bytes(TxnID txnID) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(txnID.getMostSigBits());
            dos.writeLong(txnID.getLeastSigBits());
        }
        return baos.toByteArray();
    }
}
