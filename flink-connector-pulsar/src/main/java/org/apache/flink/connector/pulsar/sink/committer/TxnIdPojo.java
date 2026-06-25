package org.apache.flink.connector.pulsar.sink.committer;

import java.util.Objects;

public class TxnIdPojo {
    private long mostSigBits;
    private long leastSigBits;

    public TxnIdPojo() {}

    public TxnIdPojo(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TxnIdPojo)) {
            return false;
        }
        TxnIdPojo txnIdPojo = (TxnIdPojo) o;
        return mostSigBits == txnIdPojo.mostSigBits && leastSigBits == txnIdPojo.leastSigBits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(mostSigBits, leastSigBits);
    }
}
