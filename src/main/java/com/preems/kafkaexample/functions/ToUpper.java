package com.preems.kafkaexample.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 9/22/15.
 */
public class ToUpper extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {

        collector.emit( new Values(tuple.getString(0).toUpperCase()));
    }
}
