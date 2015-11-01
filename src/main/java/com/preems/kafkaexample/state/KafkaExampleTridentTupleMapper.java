package com.preems.kafkaexample.state;

import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 9/23/15.
 */
public class KafkaExampleTridentTupleMapper implements TridentTupleMapper<Document<String>> {

    public Document<String> map(TridentTuple tridentTuple) {
        String index = tridentTuple.getString(0);
        String type = tridentTuple.getString(1);
        String id = tridentTuple.getString(2);
        String content = tridentTuple.getString(3);
        System.out.println("KafkaExampleTridentTupleMapper "+id);
        Document<String> esDocument = new Document<String>(index, type, content, id);
        return esDocument;
    }


}
