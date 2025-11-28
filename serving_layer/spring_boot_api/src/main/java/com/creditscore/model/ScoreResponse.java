package com.creditscore.model;

public class ScoreResponse {
    private long skIdCurr;
    private double pd1;
    private String ts;

    public ScoreResponse() {
    }

    public ScoreResponse(long skIdCurr, double pd1, String ts) {
        this.skIdCurr = skIdCurr;
        this.pd1 = pd1;
        this.ts = ts;
    }

    public long getSkIdCurr() {
        return skIdCurr;
    }

    public void setSkIdCurr(long skIdCurr) {
        this.skIdCurr = skIdCurr;
    }

    public double getPd1() {
        return pd1;
    }

    public void setPd1(double pd1) {
        this.pd1 = pd1;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }
}
