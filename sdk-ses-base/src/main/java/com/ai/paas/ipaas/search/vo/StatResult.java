package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;

public class StatResult implements Serializable {
    
    private static final long serialVersionUID = -8965221403236079982L;
    private long count;
    private double min;
    private double max;
    private double avg;
    private double sum;
    private String minTxt;
    private String maxTxt;
    private String avgTxt;
    private String sumTxt;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public String getMinTxt() {
        return minTxt;
    }

    public void setMinTxt(String minTxt) {
        this.minTxt = minTxt;
    }

    public String getMaxTxt() {
        return maxTxt;
    }

    public void setMaxTxt(String maxTxt) {
        this.maxTxt = maxTxt;
    }

    public String getAvgTxt() {
        return avgTxt;
    }

    public void setAvgTxt(String avgTxt) {
        this.avgTxt = avgTxt;
    }

    public String getSumTxt() {
        return sumTxt;
    }

    public void setSumTxt(String sumTxt) {
        this.sumTxt = sumTxt;
    }

}
