package com.flipkart.gap.pojos;

public class Range {
    // Inclusive
    long startingOffset;

    // Exclusive
    long endingOffset;

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getEndingOffset() {
        return endingOffset;
    }

    public Range(long startingOffset, long endingOffset) {
        this.startingOffset = startingOffset;
        this.endingOffset =  endingOffset;
    }

    @Override
    public String toString() {
        return "StartingOffset: " + startingOffset + " | EndingOffset: " + endingOffset;
    }

}