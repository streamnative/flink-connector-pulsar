package org.apache.flink.connector.pulsar.sink.committer;

import java.util.Map;
import java.util.Objects;

public class PulsarCommittablePojo {

    private TxnIdPojo txnID;
    private Map<String, MessageIdPojo> latestPublishedMessages;

    public PulsarCommittablePojo() {}

    public PulsarCommittablePojo(
            TxnIdPojo txnID, Map<String, MessageIdPojo> latestPublishedMessages) {
        this.txnID = txnID;
        this.latestPublishedMessages = latestPublishedMessages;
    }

    public TxnIdPojo getTxnID() {
        return txnID;
    }

    public void setTxnID(TxnIdPojo txnID) {
        this.txnID = txnID;
    }

    public Map<String, MessageIdPojo> getLatestPublishedMessages() {
        return latestPublishedMessages;
    }

    public void setLatestPublishedMessages(Map<String, MessageIdPojo> latestPublishedMessages) {
        this.latestPublishedMessages = latestPublishedMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PulsarCommittablePojo)) {
            return false;
        }
        PulsarCommittablePojo that = (PulsarCommittablePojo) o;
        return Objects.equals(txnID, that.txnID)
                && Objects.equals(latestPublishedMessages, that.latestPublishedMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txnID, latestPublishedMessages);
    }
}
