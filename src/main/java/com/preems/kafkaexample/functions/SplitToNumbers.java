package com.preems.kafkaexample.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 9/22/15.
 */
public class SplitToNumbers extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        String str = tuple.getString(0);
        String[] list = str.split(" ");

        for(String n :list) {
            try{
                collector.emit(new Values(Integer.parseInt(n)));
            }
            catch (NumberFormatException e) {
                System.out.println("Unable to format the number");
            }

        }
    }
}
