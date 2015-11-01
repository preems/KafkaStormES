package com.preems.kafkaexample.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 9/22/15.
 */
public class PrintFilter extends BaseFilter {

    public boolean isKeep(TridentTuple tuple) {

        System.out.println("Print: "+tuple.getString(0));
        return true;
    }
}
