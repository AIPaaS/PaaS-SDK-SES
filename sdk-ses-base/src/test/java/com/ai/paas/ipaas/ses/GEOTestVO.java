package com.ai.paas.ipaas.ses;

import com.ai.paas.ipaas.search.vo.GeoLocation;

public class GEOTestVO extends GeoLocation {

    /**
     * 
     */
    private static final long serialVersionUID = 1089344150577216235L;

    private String marketName;

    public String getMarketName() {
        return marketName;
    }

    public void setMarketName(String marketName) {
        this.marketName = marketName;
    }

}
