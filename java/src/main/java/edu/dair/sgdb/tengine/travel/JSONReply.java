package edu.dair.sgdb.tengine.travel;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;
import java.util.Set;

public class JSONReply {

    private JSONObject obj;
    private JSONParser parser;

    public JSONReply() {
        this.obj = new JSONObject();
        this.parser = new JSONParser();
    }

    public JSONReply add(String key, Object val) {
        this.obj.put(key, val);
        return this;
    }

    public String genString() {
        return this.obj.toString();
    }

    public byte[] genByteArray() {
        return this.obj.toString().getBytes();
    }

    public Map parse(String s) {
        try {
            this.obj = (JSONObject) this.parser.parse(s);
            Set<String> keys = this.obj.keySet();
			/*
			for (String key : keys){
				System.out.println(key + ":" + this.obj.get(key));
			}
			*/
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return this.obj;
    }
}
