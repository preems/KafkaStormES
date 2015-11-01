package com.preems.kafkaexample.functions;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.Random;

/**
 * Created by preems on 9/23/15.
 */
public class PrepareForES extends BaseFunction {

    private String esIndex,esType;

    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndex = conf.get("ELASTICSEARCH_INDEX_NAME").toString();
        this.esType = conf.get("ELASTICSEARCH_TYPE_NAME").toString();
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {

        JSONObject json = new JSONObject();
        json.put("content", tuple.getString(0));

        Random rand = new Random();
        Integer id =  Math.abs(rand.nextInt());
        collector.emit(new Values(esIndex,esType,id.toString(),json.toJSONString()));
    }
}
