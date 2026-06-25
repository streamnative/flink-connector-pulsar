package org.apache.flink.connector.pulsar.sink.committer;

import java.util.Objects;

public class MessageIdPojo {

    private int batchIndex;
    private int batchSize;
    private long ledgerId;
    private long entryId;

    public MessageIdPojo() {}

    public MessageIdPojo(long ledgerId, long entryId, int batchSize, int batchIndex) {
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public void setBatchIndex(int batchIndex) {
        this.batchIndex = batchIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MessageIdPojo)) {
            return false;
        }
        MessageIdPojo that = (MessageIdPojo) o;
        return batchIndex == that.batchIndex
                && batchSize == that.batchSize
                && ledgerId == that.ledgerId
                && entryId == that.entryId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchIndex, batchSize, ledgerId, entryId);
    }
}
