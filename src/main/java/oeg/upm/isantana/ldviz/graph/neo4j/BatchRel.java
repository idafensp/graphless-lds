package oeg.upm.isantana.ldviz.graph.neo4j;

import java.util.HashMap;
import java.util.Map;

public class BatchRel {


	private String fromid;
	private String toid;
	private String propid;
	private Map<String, Object> properties;
	

	public BatchRel(String propid, String fromid, String toid, Map<String, Object> properties) {
		super();
		this.propid = propid;
		this.fromid = fromid;
		this.toid = toid;
		this.properties = properties;
	}
	

	public BatchRel(String propid, String fromid, String toid) {
		super();
		this.propid = propid;
		this.fromid = fromid;
		this.toid = toid;
		this.properties = new HashMap<String,Object>();;
	}


	public void addProperty(String k, Object p)
	{
		this.properties.put(k, p);
	}
	
	public String getFromid() {
		return fromid;
	}
	public String getPropid() {
		return propid;
	}


	public void setPropid(String propid) {
		this.propid = propid;
	}


	public String getToid() {
		return toid;
	}
	public Map<String, Object> getProperties() {
		return properties;
	}
	public void setFromid(String fromid) {
		this.fromid = fromid;
	}
	public void setToid(String toid) {
		this.toid = toid;
	}
	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}


	@Override
	public String toString() {
		
		StringBuilder res = new StringBuilder();
		res.append("BatchRel [fromid=");
		res.append(fromid);
		res.append(", toid=");
		res.append(toid);
		res.append(", propid=");
		res.append(propid);
		res.append(", properties=");
		res.append(properties);
		res.append("]");
		
		return res.toString();
	}
	
	


	
}
