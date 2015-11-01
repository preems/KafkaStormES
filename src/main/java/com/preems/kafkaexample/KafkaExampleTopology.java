package com.preems.kafkaexample;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import com.preems.kafkaexample.filters.PrintFilter;
import com.preems.kafkaexample.functions.ToUpper;
import com.preems.kafkaexample.functions.PrepareForES;
import org.json.simple.JSONObject;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import org.elasticsearch.common.settings.Settings;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.preems.kafkaexample.state.KafkaExampleTridentTupleMapper;
import org.elasticsearch.common.settings.ImmutableSettings;

/**
 * Created by preems on 9/22/15.
 */
public class KafkaExampleTopology {


    private static String KAFKA_HOST_NAME="localhost";
    private static String KAFKA_HOST_PORT="2181";
    private static String KAFKA_TOPIC_NAME="exampletopic";
    private static String ELASTICSEARCH_CLUSTER_NAME="elasticsearch";
    private static String ELASTICSEARCH_HOST_NAME="localhost";
    private static String ELASTICSEARCH_HOST_PORT="9300";
    private static String ELASTICSEARCH_INDEX_NAME="kafkaexample_index";
    private static String ELASTICSEARCH_TYPE_NAME="kafkaexample_type";


    public static StormTopology buildTopology(Config conf, LocalDRPC localDrpc) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts(KAFKA_HOST_NAME + ":" +KAFKA_HOST_PORT);
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, KAFKA_TOPIC_NAME);
        kafkaConfig.startOffsetTime=kafka.api.OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //kafkaConfig.ignoreZkOffsets=true;
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);

        //ElasticSearch Persistent State
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", ELASTICSEARCH_CLUSTER_NAME)
                .put("storm.elasticsearch.hosts", ELASTICSEARCH_HOST_NAME + ":" + ELASTICSEARCH_HOST_PORT)
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<JSONObject>(new ClientFactory.NodeClient(esSettings.getAsMap()), JSONObject.class);
        //ESIndexMapState.Factory<JSONObject> esStateFactory = ESIndexMapState.nonTransactional(new ClientFactory.NodeClient(esSettings.getAsMap()),JSONObject.class);

        TridentState esStaticState = topology.newStaticState(esStateFactory);

        conf.put("ELASTICSEARCH_INDEX_NAME",ELASTICSEARCH_INDEX_NAME);
        conf.put("ELASTICSEARCH_TYPE_NAME", ELASTICSEARCH_TYPE_NAME);

        topology.newStream("kafkaExampleStream",spout).parallelismHint(1)
                //.each(new Fields("str"),new SplitToNumbers(),new Fields("numbers"))
                //.each(new Fields("numbers"), new ZeroFilter())
                .each(new Fields("str"), new ToUpper(), new Fields("upperString"))
                .each(new Fields("upperString"), new PrintFilter())
                .each(new Fields("upperString"), new PrepareForES(), new Fields("index", "type", "id", "content"))
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "content"), new ESIndexUpdater<String>(new KafkaExampleTridentTupleMapper()), new Fields());
                ;
        return topology.build();
    }

    public static void main(String args[]) {

        Config conf = new Config();
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster localcluster = new LocalCluster();
        localcluster.submitTopology("kafkaExampleTopology",conf,buildTopology(conf, drpc));
    }
}
