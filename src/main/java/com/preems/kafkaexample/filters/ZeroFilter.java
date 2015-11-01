package com.preems.kafkaexample.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 9/22/15.
 */
public class ZeroFilter extends BaseFilter {

    public boolean isKeep(TridentTuple tuple) {
        if(tuple.getInteger(0)==0)
            return false;
        else
            return true;
    }

}
