package oeg.upm.isantana.ldviz.utils;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import oeg.upm.isantana.ldviz.graph.neo4j.BatchNode;
import oeg.upm.isantana.ldviz.graph.neo4j.BatchRel;
import oeg.upm.isantana.ldviz.graph.neo4j.Constants;
import oeg.upm.isantana.ldviz.sparql.RDFSLabels;
import oeg.upm.isantana.ldviz.sparql.RDFTypes;

public class DataWriter {
	
	private String path;
	private Model mod;
	private RDFTypes rdft;
	private RDFSLabels rdfslab;
	private Map<String, Integer> resIngoingDegree;
	private Map<String, Integer> resOutgoingDegree;
	private ArrayList<BatchNode> nodeBatch;
	private ArrayList<BatchRel> propBatch;
	private Property propInDegree;
	private Property propOutDegree;
	private Property propDegree;
	
	
	public DataWriter(String path, RDFTypes rdft, RDFSLabels rdfslab, Map<String, Integer> resIngoingDegree,
			Map<String, Integer> resOutgoingDegree, ArrayList<BatchNode> nodeBatch, ArrayList<BatchRel> propBatch) {
		super();
		this.path = path;
		this.mod = ModelFactory.createDefaultModel();
		this.rdft = rdft;
		this.rdfslab = rdfslab;
		this.resIngoingDegree = resIngoingDegree;
		this.resOutgoingDegree = resOutgoingDegree;
		this.nodeBatch = nodeBatch;
		this.propBatch = propBatch;
		

	    this.propInDegree = mod.createProperty(Constants.PROP_NODE_IN_DEGREE);
	    this.propOutDegree = mod.createProperty(Constants.PROP_NODE_OUT_DEGREE);
	    this.propDegree = mod.createProperty(Constants.PROP_NODE_DEGREE);
	}
	
	//plainly writing nodes and rels to file, no special formatting
	public boolean writeDataNodes()
	{
		System.out.print("Stringify " + nodeBatch.size() + " nodes to file");

    	
        ProgressBar bar1 = new ProgressBar(nodeBatch.size(),100);

    	
    	StringBuilder res = new StringBuilder();
		for(BatchNode bn : this.nodeBatch)
		{
			//https://stackoverflow.com/questions/1126388/slow-string-concatenation-over-large-input
			res.append(bn.toString());
			res.append("\n");
			bar1.update();

		}
		
		this.stringToFile(path+"_batch_nodes.txt", res.toString());
		return true;
	}
	public boolean writeDataRels()
	{
		

    	StringBuilder res = new StringBuilder();

		System.out.print("\nStringify " + propBatch.size() + " properties to file");
        ProgressBar bar2 = new ProgressBar(propBatch.size(),100);

		for(BatchRel bp : this.propBatch)
		{
			res.append(bp.toString());
			res.append("\n");

			bar2.update();
		}
		
		this.stringToFile(path+"_batch_rels.txt", res.toString());
		return true;
	}
	
	
	
	//sofisticated, using ttl and custom vocab
	//TODO tbd
	public boolean writeDataTriples() throws IOException
	{
		for(BatchNode bn : this.nodeBatch)
		{
			String uri = (String) bn.getProperties().get(Constants.URI);
			Resource nodeInd = mod.createResource(uri);
			
			if(!bn.isLiteral())
			{
				System.out.print("adding types for: " + bn);
				this.addResroucceTypes(nodeInd, bn);
			}
			
			//TODO complete this method
			//TODO what to to with literals? they don't have URI, so ingoing degree, etc.?
			
			
		}
		
		FileWriter out = new FileWriter(path);
		try {
		    mod.write( out, "TTL" );
		}
		finally {
		   try {
		       out.close();
		   }
		   catch (IOException closeException) {
		       // ignore
			   return false;
		   }
		}
		
		return true;
	}
	
	private boolean addResroucceTypes(Resource nodeInd, BatchNode bn)
	{
		
		String  tString = (String) bn.getProperties().get(Constants.TYPE_LIST);
		List<String> types = this.stringToList(tString);
		
		for(String tp : types)
		{
			//add type to the resource on the model
			Resource ntype = mod.createResource(tp);
			mod.add(nodeInd, RDF.type, ntype);
		}
		
		return true;
	}
	
	
	private List<String> stringToList(String in)
	{
		
		System.out.println("types="+in);
		String num = in.substring(1,in.length()-1);
        String str[] = num.split(",");
        
        List<String> al = new ArrayList<String>();
        al = Arrays.asList(str);
        return al;
	}
	
	
	private void stringToFile(String path, String content)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(content);
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
