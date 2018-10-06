package edu.dair.sgdb.tengine.travel;

import org.json.simple.JSONObject;

import java.util.Map;

public class SingleStep {

	/*
	private SingleRelation vertexKeyRel = null;	
	private List<SingleRelation> vertexStaticAttrRels = null;
	private List<SingleRelation> vertexDynAttrRels = null;
	private SingleRelation typeRel = null;
	private List<SingleRelation> edgeRels = null;
	*/

    public Restriction vertexKeyRestrict = null;
    public Restriction vertexStaticAttrRestrict = null;
    public Restriction vertexDynAttrRestrict = null;
    public Restriction typeRestrict = null;
    public Restriction edgeKeyRestrict = null;

    @Override
    public String toString() {
        String str = "vertexKeyRestrict: " + this.vertexKeyRestrict + " vertexStaticAttrRestrict: " + this.vertexStaticAttrRestrict
                + " vertexDynAttrRestrict: " + this.vertexDynAttrRestrict + " typeRestrict: " + this.typeRestrict
                + " edgeKeyRestrict: " + this.edgeKeyRestrict;
        return str;
    }

    public JSONObject genJSON() {
        JSONObject obj = new JSONObject();
        if (vertexKeyRestrict != null)
            obj.put("vertex_key_restrict", vertexKeyRestrict.genJSON());
        if (vertexStaticAttrRestrict != null)
            obj.put("vertex_static_attr_restrict", vertexStaticAttrRestrict.genJSON());
        if (vertexDynAttrRestrict != null)
            obj.put("vertex_dyn_attr_restrict", vertexDynAttrRestrict.genJSON());
        if (typeRestrict != null)
            obj.put("edge_type_restrict", typeRestrict.genJSON());
        if (edgeKeyRestrict != null)
            obj.put("edge_key_restrict", edgeKeyRestrict.genJSON());
        return obj;

    }

    public static SingleStep parseJSON(String json) {
        SingleStep stp = new SingleStep();
        JSONCommand js = new JSONCommand();
        Map r = js.parse(json);
        if (r.get("vertex_key_restrict") != null) {
            JSONObject sVertexKeyRestrict = (JSONObject) r.get("vertex_key_restrict");
            stp.vertexKeyRestrict = SingleRestriction.parseJSON(sVertexKeyRestrict.toString());
        }
        if (r.get("vertex_static_attr_restrict") != null) {
            JSONObject obj = (JSONObject) r.get("vertex_static_attr_restrict");
            stp.vertexStaticAttrRestrict = SingleRestriction.parseJSON(obj.toString());
        }
        if (r.get("vertex_dyn_attr_restrict") != null) {
            JSONObject obj = (JSONObject) r.get("vertex_dyn_attr_restrict");
            stp.vertexDynAttrRestrict = SingleRestriction.parseJSON(obj.toString());
        }
        if (r.get("edge_type_restrict") != null) {
            JSONObject obj = (JSONObject) r.get("edge_type_restrict");
            stp.typeRestrict = SingleRestriction.parseJSON(obj.toString());
        }
        if (r.get("edge_key_restrict") != null) {
            JSONObject obj = (JSONObject) r.get("edge_key_restrict");
            stp.edgeKeyRestrict = SingleRestriction.parseJSON(obj.toString());
        }
        return stp;
    }

}
