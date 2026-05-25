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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.transaction.TxnID;

import java.util.Objects;

/** The writer state for Pulsar connector. We would used in Pulsar committer. */
@PublicEvolving
public class PulsarCommittable {

    /** The transaction id. */
    private final TxnID txnID;

    /**
     * To ensure compatibility after degradation, the new version can still restore the PulsarCommittable
     * object already stored in the old version and keep this constructor.
     * @deprecated the param @param topic is meaningless now.
     */
    @Deprecated
    public PulsarCommittable(TxnID txnID, String topic) {
        this(txnID);
    }

    public PulsarCommittable(TxnID txnID) {
        this.txnID = txnID;
    }

    public TxnID getTxnID() {
        return txnID;
    }

    /**
     * @deprecated the param @param topic is meaningless now.
     */
    @Deprecated
    public String getTopic() {
        return PulsarCommittableSerializer.TOPIC_PLACEHOLDER;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarCommittable that = (PulsarCommittable) o;
        return Objects.equals(txnID, that.txnID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnID);
    }

    @Override
    public String toString() {
        return "PulsarCommittable{" + "txnID=" + txnID + '}';
    }
}
