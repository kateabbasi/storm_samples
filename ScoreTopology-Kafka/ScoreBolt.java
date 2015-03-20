package com.digitalsandbox.app.common.storm.bolt;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import com.digitalsandbox.app.common.DBConnection;
import com.digitalsandbox.app.common.model.Model;
import com.digitalsandbox.app.common.model.ScoreModel;
import com.mongodb.DB;
import com.mongodb.MongoException;

/**
 * ScoreBolt calculates threat score and other scores relevant to identifying how "trustworthy" information is in text. ScoreBolt is reused by other
 * topologies such as TwitterTopology and expects incoming tuples to have header, description and msgid fields.
 * 
 * 
 * @author Kate Abbasi 
 * 
 */

public class ScoreBolt extends BaseRichBolt {
	private static final long serialVersionUID = 8723875722994575820L;
	
	public static Logger log = null;
   
	OutputCollector _collector;
    Fields _idFields;
    Fields _outStreams;
    Fields _outFields;
    int _numSources;
    int _numOfNamedEntities, _numberofsuscribers;
    Boolean _print;
    Map<String, GlobalStreamId> _fieldLocations;
    
	DB db = null;
	DB archive_db = null;
	ScoreModel _scoreModel;
	Model _model;
	
	private Fields _fieldsList = new Fields();
	
    public ScoreBolt(Fields outputFields) {
    	log = Logger.getLogger(ScoreBolt.class); 
    	
    	if(outputFields != null)
			_fieldsList = outputFields;
        
    }

    
    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	try {
			Class.forName("com.mongodb.Mongo");
		} catch (ClassNotFoundException e) {
			System.err.println("EXCEPTION: Could not find the MongoDB driver!");
		}   	
    	
    	_collector = collector;
    	
    	_scoreModel = new ScoreModel();
    	
    	try {
    		db = DBConnection.InitializeDB(conf);
    		
    		_model = new Model(db, "threat", true, true);
    		
    		//MongoDB can be run in a secure mode where access to databases is controlled through name and password authentication.
    		//Most users run MongoDB without authentication in a trusted environment.
    		//boolean auth = db.authenticate("db", "password");
		} catch (UnknownHostException e) {
			System.err.println("ScoreBolt could not connect to " + conf.get("dbserver").toString());
			_collector.reportError(e); //show error in Storm UI
		} catch (MongoException e) {
			System.err.println("ScoreBolt MongoException ");
			_collector.reportError(e); //show error in Storm UI
		}
    }


    @SuppressWarnings("unchecked")
	@Override
    public void execute(Tuple tuple) {
    	
    	
    	try {
            
    	   // See if the model needs to be udpated
    	   if (_model.doesModelNeedUpdating()) {
    		   _model.loadModel(true);
    	   }   
    	   
    	   if ((tuple.contains("numberofsuscribers")) && (tuple.getValueByField("numberofsuscribers") != null))
           {
    		   _numberofsuscribers = tuple.getIntegerByField("numberofsuscribers");
           }
    	   else
    		   _numberofsuscribers =-1 ;
 
           String msgid 			= tuple.getStringByField("msgid");
		   String header            = tuple.getStringByField("header");
		   String description       = tuple.getStringByField("description");
		   
		   String text = header + " " + description;
		   //process text to find all matching topics
           _model.classify(text);
          
          
		  // this stores the de-duped threats
          Set<String> threats= _model.getTopics();
          
          // this stores every node that was triggered, and its ancestors
          // this is better for UI display
          ArrayList<ArrayList<String>> threatbranches= _model.getTopicLists(); 
          //this stores a list of words in text that triggered match on threat
          ArrayList<ArrayList<String>> threattriggerwords= _model.getTopicTriggerWordList(); 
          
          _scoreModel.score(header, 
        		  			description,  
        		  			_numberofsuscribers, 
        		  			_model.getScore());
          
          double contentscore=   _scoreModel.getScore();
          double objectivityscore = _scoreModel.getObjectivityScore();
          double credibilityscore =  _scoreModel.getCredibilityScore();
          double impersonalscore =   _scoreModel.getImpersonalScore();
          double formalityscore =   _scoreModel.getFormalityScore();
          double specificityscore = _scoreModel.getSpecificityScore();
          double threatscore =  _model.getScore();
          double person =   _scoreModel.getPerson();
          
          ArrayList<Object> values = (ArrayList<Object>) tuple.getValues();
		  values.add(threats);
		  values.add(threatbranches);
		  values.add(threattriggerwords);
		  values.add(contentscore);
		  values.add(objectivityscore);
		  values.add(credibilityscore);
		  values.add(impersonalscore);
		  values.add(formalityscore);
		  values.add(specificityscore);
		  values.add(threatscore);
		  values.add(person);
		  
			
		  _collector.emit(tuple, values);
		  
           // every tuple you process must be acked or failed. 
           // storm uses memory to track each tuple, so if you don't ack/fail every tuple, the task will eventually run out of memory.
           _collector.ack(tuple);
			
		} 
    	
    	catch(Throwable t) {
    		_collector.reportError(t); //show error in Storm UI
			// every tuple you process must be acked or failed. 
	        // storm uses memory to track each tuple, so if you don't ack/fail every tuple, the task will eventually run out of memory.
    		
    		_collector.fail(tuple);
    	
		} 
    }
    
  
    @Override
	/*cleanup is never called on the cluster, just in local mode
	 * (non-Javadoc)
	 * @see backtype.storm.topology.base.BaseRichBolt#cleanup()
	 */
	public void cleanup() {
    	if(db.getMongo() != null) db.getMongo().close();
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	
    	List<String> fields = _fieldsList.toList(); 
		fields.add("threats");
		fields.add("threatbranches");
		fields.add("threattriggerwords");
		fields.add("contentscore");
		fields.add("objectivityscore");
		fields.add("credibilityscore");
		fields.add("impersonalscore");
		fields.add("formalityscore");
		fields.add("specificityscore");
		fields.add("threatscore");
		fields.add("person");
		
		declarer.declare(new Fields(fields)); 
		
    }
    
    
}
