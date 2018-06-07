package oeg.upm.isantana.ldviz.graph.neo4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class BatchNode {
	
	private String id;
	private boolean isLiteral;
	private Map<String, Object> properties;

	public BatchNode(String id, Map<String, Object> properties) {
		super();
		this.id = id;
		this.properties = properties;
		this.isLiteral =false;
	}


	public BatchNode(String id) {
		super();
		this.id = id;
		this.properties = new HashMap<String,Object>();
		this.isLiteral =false;
	}

	public BatchNode(String id, boolean lit) {
		super();
		this.id = id;
		this.properties = new HashMap<String,Object>();
		this.isLiteral = lit;
	}

	
	public void addPropertyString(String k, String p)
	{
		this.properties.put(k, p);
	}
	
	public void addPropertyDouble(String k, Double p)
	{
		this.properties.put(k, p);
	}
	
	public void addPropertyInteger(String k, Integer p)
	{
		this.properties.put(k, p);
	}
	
	public void addPropertyList(String k, HashSet<String> p)
	{
		this.properties.put(k, p.toString());
	}
	
	public String getId() {
		return id;
	}
	public Map<String, Object> getProperties() {
		return properties;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}
	

	public boolean isLiteral() {
		return isLiteral;
	}


	public void setLiteral(boolean isLiteral) {
		this.isLiteral = isLiteral;
	}
	

	@Override
	public String toString() {
		
		StringBuilder res = new StringBuilder();
		res.append("BatchNode [id=");
		res.append(id);
		res.append(", isLiteral=");
		res.append(isLiteral);
		res.append(", properties=");
		res.append(properties);
		res.append("]");
		
		return  res.toString();
	}


	

}
