package com.digitalsandbox.app.common;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Deserialize JSON formatted string into List of values using specified String array of fields
 * 
 * @author Kate Abbasi 
 * 
 */

public class JSONObjectScheme implements Scheme {
  
 
	private static final long serialVersionUID = 1L;

	public static final String STRING_SCHEME_KEY = "str";
    
    Fields _fields = null;
    
    public JSONObjectScheme (String[] fieldsArray)
    {
    	
    	_fields = new Fields(fieldsArray);
    }
    public List<Object> deserialize(byte[] bytes) {
       
    	String msg = deserializeString(bytes);
    	try{ 

	    	JSONObject obj = new JSONObject(msg); 
			  
		    ArrayList<Object> values = new ArrayList<Object>();  
			for(int x=0; x < _fields.size(); x ++)
			{
				values.add(obj.get(_fields.get(x)));
			}
			return values;
       }catch(JSONException ex){
    	   
    	   System.err.println(ex.getMessage() + ": " + msg);
    	   return null;
       }
    
    }
    
    public static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
    	
        return _fields;
    }
}