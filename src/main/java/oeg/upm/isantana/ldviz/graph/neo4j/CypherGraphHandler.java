package oeg.upm.isantana.ldviz.graph.neo4j;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;

import oeg.upm.isantana.ldviz.utils.HashNodes;

public class CypherGraphHandler {
	
	private String graphName;

	
	public CypherGraphHandler(String gn)
	{
		this.graphName = gn;
	}
	

	public String generateCypherNodeQuery(String uri, String label, List<String> types) throws NoSuchAlgorithmException
	{
		String labelString ="";
		if(!label.isEmpty())
		{
			labelString = "label: '"+label+"',";
		}
		
		String typeList = "";
		if((types!=null)&&(!types.isEmpty()))
		{
			typeList = ", types: ['" + this.getTypesListString(types) +"'] ";
		}
		
		String nodeid = HashNodes.hashNodeUri(uri, this.graphName);
		
		
		String query = "MERGE ("+ nodeid+":"+graphName+" { "+labelString+" uri: '"+uri+"' "+ typeList +" , nodeid: '"+nodeid+"'}) RETURN "+ nodeid;
		return query;
	}
	
	public String generateCypherNodeQueryLiteral(String id, String value) throws NoSuchAlgorithmException
	{
		String labelString ="";
		String typeList = "";
		
		
		
		String query = "MERGE ("+ id+":"+graphName+":Literal {value: '"+value+"' , nodeid: '"+id+"'}) RETURN "+ id;
		return query;
	}
	
	public String generateCypherRelationshipQuery(String propUri, String uriOr, String uriTar, String label, double weight) throws NoSuchAlgorithmException
	{
		String query ="";

		String labelString ="";
		if(!label.isEmpty())
		{
			labelString = "label: '"+label+"',";
		}

		String nor = HashNodes.hashNodeUri(uriOr, this.graphName);
		String nta = HashNodes.hashNodeUri(uriTar, this.graphName);
		
		query = "MATCH (a:"+graphName+"),(b:"+graphName+")" +
		" WHERE a.nodeid = '"+nor+"' AND b.nodeid = '"+nta+"'"+
		" MERGE (a)-[r:"+graphName+" {"+labelString+"  uri: '"+propUri+"', weight: "+weight+"}]->(b)"+
		" RETURN r";
		
		return query;
	}
	
	
	public BatchNode generateBatchNode(String uri, String label, HashSet<String> types) throws NoSuchAlgorithmException
	{
		BatchNode bn = new BatchNode(HashNodes.hashNodeUri(uri, this.graphName));
		
		if(!label.isEmpty())
			bn.addPropertyString(Constants.LABEL, label);
		
		bn.addPropertyString(Constants.NODE_ID, bn.getId());
		bn.addPropertyString(Constants.URI, uri);
		bn.addPropertyList(Constants.TYPE_LIST, types);
		
		return bn;
	}

	
	
	public BatchNode generateBatchLiteralNode(String nodeId, String value) throws NoSuchAlgorithmException
	{
		BatchNode bn = new BatchNode(nodeId, true);
		
		
		bn.addPropertyString(Constants.NODE_ID, bn.getId());
		bn.addPropertyString(Constants.VALUE, value);
		bn.addPropertyString(Constants.LABEL, value);
		
		return bn;
	}
	

	public BatchRel generateBatchRelationship(String propUri, String idOr, String idTar, String label, double weight, String di) throws NoSuchAlgorithmException
	{
		String propid = HashNodes.hashPropUri(propUri, this.graphName);
		BatchRel bp = new BatchRel(propid, idOr, idTar);
		
		if(!label.isEmpty())
			bp.addProperty("label", label);
		
		bp.addProperty("propid", propid);
		bp.addProperty("weight", (Double) weight);
		bp.addProperty("uri", propUri);
		bp.addProperty("dir", di);
		return bp;
	}
	
	
	private String getTypesListString(List<String> list)
	{
		String idList = list.toString();
		String csv = idList.substring(1, idList.length() - 1).replace(", ", "','");
		return csv;
	}
	

}
