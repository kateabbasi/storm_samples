package com.digitalsandbox.app.computation.storm;

import storm.kafka.*;

import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.*;

import com.digitalsandbox.app.common.JSONObjectScheme;
import com.digitalsandbox.app.common.storm.bolt.ScoreBolt;

import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Properties;

/**
 * ScoreTopology read messages from Kafka queue, converts them to JSON tuples for processing by ScoreBolt and finally converts tuples
 * emitted from ScoreBolt to messages and forwards them to Kafka.
 * 
 * ScoreBolt calculates threat score and other scores relevant to identifying how "trustworthy" information is in text. ScoreBolt is reused by other
 * topologies such as TwitterTopology and expects incoming tuples to have header, description and msgid fields.
 * 
 * Incoming messages are consumed from "score-in" topic. The messages should be formatted as JSON and have header, description and msgid fields
 * 
 * ex.: {"msgid":54fde996e4b05b771314639f, "description":"Soldiers from Chad and Niger launched the largest international push to 
 * defeat Nigeria&#39;s Islamic extremists whose war has spilled over into neighboring countries, 
 * officials and witnesses said Monday. Chad&#39;s...", "header": "Chad and Niger troops attack Nigeria's Boko Haram extremists"}
 * 
 * Outgoing messages are sent to "score-out" topic. Use msgid to map ScoreBolt output to original message.
 * 
 * ex.:{"credibilityscore":1,"person":3,"msgid":"54fde996e4b05b771314639f","threatscore":0.4,"impersonalscore":0,
 * "threattriggerwords":[["extremist"]],"specificityscore":0.20666666666666667,
 * "threats":["Person of Interest","Intentional","Extremist","Threat"],"header":"Chad and Niger troops attack Nigeria's Boko Haram extremists",
 * "objectivityscore":0.8333333333333334,"description":"Soldiers from Chad and Niger launched the largest international push to defeat 
 * Nigeria&#39;s Islamic extremists whose war has spilled over into neighboring countries, officials and witnesses said Monday. 
 * Chad&#39;s...","formalityscore":1,"contentscore":0.42080000000000006,"threatbranches":[["Extremist","Person of Interest","Intentional","Threat"]]}
 * 
 * @author Kate Abbasi 
 * 
 */

public class ScoreTopology {
    public static final Logger LOG = LoggerFactory.getLogger(ScoreTopology.class);

    public static class MessageToTupleBolt extends BaseRichBolt {
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private OutputCollector _collector;
    	
    	@Override
    	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    		_collector      = collector;
			
		}
    	
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	declarer.declare(new Fields("header", "description", "msgid"));
        }

        @Override
        public void execute(Tuple tuple) {
        	try{
	            //LOG.info(tuple.toString());
	            
	            JSONArray message = new JSONArray(tuple.getStringByField("str"));
	            JSONObject obj = (JSONObject) message.get(0);
	     		  
	            _collector.emit(tuple, new Values(obj.get("header").toString(), obj.get("description").toString(), obj.get("id").toString()));
				_collector.ack(tuple);
			
			}catch(Throwable t) {
				System.err.println("MessageToTupleBolt threw an exception in its execute method: " + t.getMessage());
				_collector.reportError(t); //show error in Storm UI
				_collector.fail(tuple); //have to ack() not fail() here, to keep the failed message from being replayed indefinitely
			}
        }
    }

    public static class TupleToMessageBolt extends BaseRichBolt {
    	/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private OutputCollector _collector;
    	
    	@Override
    	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    		_collector      = collector;
			
		}
    	
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	declarer.declare(new Fields("message"));
        }

        @Override
        public void execute(Tuple tuple) {
        	try{
	            //LOG.info(tuple.toString());
	            JSONObject obj = new JSONObject();
	            
	            
	            for (String field : tuple.getFields()) {
	                if (tuple.getValueByField(field) instanceof org.json.simple.JSONArray)
	                	obj.put(field, ((org.json.simple.JSONArray)tuple.getValueByField(field)).toJSONString());
	                else
	                	obj.put(field, tuple.getValueByField(field));
	            }
	            
	            
	            
	     		  
	            _collector.emit(tuple, new Values(obj.toString()));
				_collector.ack(tuple);
			
			}catch(Throwable t) {
				System.err.println("MessageToTupleBolt threw an exception in its execute method: " + t.getMessage());
				_collector.reportError(t); //show error in Storm UI
				_collector.ack(tuple); //have to ack() not fail() here, to keep the failed message from being replayed indefinitely
			}
        }
		

    }

    private final BrokerHosts brokerHosts;

    public ScoreTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "score-in", "/kafkastorm", "storm");
        //kafkaConfig.forceFromStart= true; //will start fetching from beginning of queue, which is cleared out each time kafka server is started
        //messages are converted to JSON tuple using JSONObjectScheme.
        kafkaConfig.scheme = new SchemeAsMultiScheme(new JSONObjectScheme(new String[] {"header", "description", "msgid"}));
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fromKafka", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("ScoreBolt", new ScoreBolt(new Fields("header", "description", "msgid"))).shuffleGrouping("fromKafka");
        builder.setBolt("TupleToMessageBolt", new TupleToMessageBolt()).shuffleGrouping("ScoreBolt");       
        builder.setBolt("forwardToKafka", new KafkaBolt<Object, Object>()).shuffleGrouping("TupleToMessageBolt");
        
        
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        
        Config config = new Config();
        String database;
    	String dbserver;
    	String dbport;
    	String dbuser;
    	String dbpassword ;
    	String useSSL;
    	String kafkaZk;
    	int numWorkers =0;
    	
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        
        config.put("topic", "score-out"); //producer output topic
        
        
        
        
        Properties prop = new Properties();		

        if (args != null && args.length > 0) { //topology is being deployed to cluster
        	
        	if (args.length < 2) 
				throw new InvalidParameterException("Must provide at least two parameters: Topology Name and Config Properties file");
			
        	prop.load(ScoreTopology.class.getResourceAsStream("/config-" + args[1] + ".properties"));
        	
            String name = args[0];
            
            //set producer properties.
            Properties props = new Properties();
            props.put("metadata.broker.list",prop.getProperty("metadata.broker.list", "localhost:9092"));
            props.put("request.required.acks", "1");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            
            config.put("kafka.broker.properties", props);
            
            
            //store db connection info in storm config
            //ScoreBolt connects to db to extract threat model
    		database = prop.getProperty("database", "castle");
    		dbserver=prop.getProperty("dbserver", "localhost");  
    		dbport=prop.getProperty("dbport", "27017");
    		dbuser=prop.getProperty("dbuser", "");
    		dbpassword=prop.getProperty("dbpassword", "");
    		useSSL=prop.getProperty("useSSL", "false");
    		
    		//store db connection info in storm config
    		config.put("dbserver", dbserver);
    		config.put("database", database);
        	config.put("dbport", dbport);
    		config.put("dbuser", dbuser);
        	config.put("dbpassword", dbpassword);
        	config.put("useSSL", useSSL);
        	
        	numWorkers = Integer.parseInt( prop.getProperty("numWorkers", "1"));
        	config.setNumWorkers(numWorkers);
            
        	kafkaZk = prop.getProperty("kafkaZk", "localhost:2181");
        	
        	
    		ScoreTopology kafkaSpoutTopology = new ScoreTopology(kafkaZk);
            StormTopology stormTopology = kafkaSpoutTopology.buildTopology();
        	
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else { //topology is running in Eclipse
        	
        	prop.load(ScoreTopology.class.getResourceAsStream("/config.properties"));
        	
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            
           //set producer properties.
            Properties props = new Properties();
            props.put("metadata.broker.list",prop.getProperty("metadata.broker.list", "localhost:9092"));
            props.put("request.required.acks", "1");
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            
            config.put("kafka.broker.properties", props);
            
            //store db connection info in storm config
    		database = prop.getProperty("database", "castle");
    		dbserver=prop.getProperty("dbserver", "localhost");  
    		dbport=prop.getProperty("dbport", "27017");
    		dbuser=prop.getProperty("dbuser", "");
    		dbpassword=prop.getProperty("dbpassword", "");
    		useSSL=prop.getProperty("useSSL", "false");
    		
    		//store db connection info in storm config
    		config.put("dbserver", dbserver);
    		config.put("database", database);
        	config.put("dbport", dbport);
    		config.put("dbuser", dbuser);
        	config.put("dbpassword", dbpassword);
        	config.put("useSSL", useSSL);
        	
        	kafkaZk = prop.getProperty("kafkaZk", "localhost:2181");
    		ScoreTopology kafkaSpoutTopology = new ScoreTopology(kafkaZk);
            StormTopology stormTopology = kafkaSpoutTopology.buildTopology();
        	
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }
}