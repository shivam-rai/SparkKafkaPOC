package com.flipkart.gap.pojos;

public class EpochOffsetTuple {
    public long getOffset() {
        return offset;
    }

    public long getEpoch() {
        return epoch;
    }

    private long epoch;
    private long offset;

    public EpochOffsetTuple(long epoch, long offset) {
        this.epoch = epoch;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Epoch: " + epoch + " | Offset: " + offset;
    }
}
