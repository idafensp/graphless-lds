package oeg.upm.isantana.ldviz.sparql.sql;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;

import oeg.upm.isantana.ldviz.sparql.RDFTypes;
import oeg.upm.isantana.ldviz.sparql.SPARQLSource;
import oeg.upm.isantana.ldviz.utils.Counter;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;



public class TypesPropertyCountSQL  extends SPARQLSource {
	
//	private Map<String, Map<String, Integer>>  outgoingProps;
//	private Map<String, Map<String, Integer>>  ingoingProps;
	
	private NotRelevantProperties nrp;
	private RDFTypes rdft;
	private SQLHelper sqlh;
	
	public TypesPropertyCountSQL(String seu, String ng, NotRelevantProperties notrp, RDFTypes r, String graphName)
	{
		super(seu, ng);
		this.rdft = r;
		this.nrp = notrp;
		sqlh = new SQLHelper(graphName);

	}
	
	public TypesPropertyCountSQL(String seu, String ng, NotRelevantProperties notrp, RDFTypes r, int lim, int off, int w, String graphName) 
	{
		super(seu, ng, lim, off, w);
		this.rdft = r;
		this.nrp = notrp;
		sqlh = new SQLHelper(graphName);

	}
	
	public boolean ingoingPropertiesCount()
	{
		
		sqlh.createTableRTPCount(SQLConstants.RTP_COUNT_ING);
		
		System.out.println("ingoingPropertiesCount");
		

		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		
		String queryString = "	SELECT ?p (count(?p) AS ?num)\n"  + fromClause + "\n"
				+ "	WHERE \n" 
				+ "	{ \n" 
				+ "    SELECT ?p \n"
				+ "	   WHERE \n" 
				+ "	   { \n" 
				+ "	       ?s ?p <@@RURI@@>  .\n" 
				+ "	   } ORDER BY ?p\n"
				+ "	}  GROUP BY ?p OFFSET @@START@@ LIMIT " + this.limit + "\n";
		
		return getPropertiesCount(queryString, SQLConstants.RTP_COUNT_ING);
	}
	
	
	public boolean outgoingPropertiesCount()
	{
		

		sqlh.createTableRTPCount(SQLConstants.RTP_COUNT_OUT);
		
		System.out.println("outgoingPropertiesCount");
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		String queryString = "	SELECT ?p (count(?p) AS ?num)\n"  + fromClause + "\n"
				+ "	WHERE \n" 
				+ "	{ \n" 
				+ "    SELECT ?p \n"
				+ "	   WHERE \n" 
				+ "	   { \n" 
				+ "	       <@@RURI@@> ?p  ?o .\n" 
				+ "	   } ORDER BY ?p\n"
				+ "	} GROUP BY ?p  OFFSET @@START@@ LIMIT " + this.limit + "\n";
		
		return getPropertiesCount(queryString, SQLConstants.RTP_COUNT_OUT);
	}

	
	
	public boolean getPropertiesCount(String queryString, String tableName)// , Map<String, Map<String, Integer>> props)
	{
		
		int startQuery = this.offset;
		
		
		//loop all resources
		Set<String> ks = this.rdft.getResourceTypes().keySet();
		
		System.out.println("* Inspecting "+ ks.size()+" resources");
		
		int gpc =0;
		for(String ruri : ks)
		{
			
			boolean loop = true;	
			while (loop) 
			{

				String paramQueryString = queryString.replace("@@RURI@@", ruri);
				paramQueryString = paramQueryString.replace("@@START@@", String.valueOf(startQuery));

				System.out.println(gpc++ + "/" + ks.size()+") getPropertiesCount - Query for:"+ruri+"\n" + paramQueryString);
				Query query = QueryFactory.create();
				
				try {
					QueryFactory.parse(query, paramQueryString, "", Syntax.syntaxSPARQL_11);
				} catch (org.apache.jena.query.QueryParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					addParsingError(ruri,paramQueryString);
					continue;
				} catch (Exception e1) {
					addParsingError(ruri,paramQueryString);
					e1.printStackTrace();
					continue;
				}
				
				
				System.out.println("About to query...");
				QueryExecution qexec = QueryExecutionFactory.sparqlService(this.sparqlEndpointUrl, query);
				
				ResultSet results = null;
				try
				{
					System.out.println("...about to exec... " + ruri);

					Counter.increase();
					results = qexec.execSelect(); 
					System.out.println("...queried!");
				}
				catch (Exception e){
					System.out.println("Found no results exception");
					e.printStackTrace();
					return false; // :(
				}
				
				if(!results.hasNext())
				{
					System.out.println("Found no results");
					loop = false; //quite unnecessary right?
					break;
				}
				
				//get all the types of the resource
				HashSet<String> typeList = this.rdft.getTypeList(ruri);
				
				int tc = 0;
				
				System.out.println("Get results...");
				while (results.hasNext()) 
				{
					QuerySolution binding = results.nextSolution();
					Resource prop = (Resource) binding.get("p");
					Literal count = binding.getLiteral("num");

					
					if (prop==null)
					{
						System.out.println("** ** ** Not prop found for="+ruri + "\n" + paramQueryString);
						continue;	
					}
					if (nrp.irrelevant(prop.getURI()))
					{
						continue;	
					}


					
					tc++;
					
					//System.out.println(type.getLocalName()+"("+ prop.getLocalName() +")=" +count.getInt());


					System.out.println("SQL ready for " + typeList.size() + " types");
					for (String type : typeList) {
						
						
						sqlh.insertRTPCount(tableName, ruri, type, prop.getURI(), count.getInt());
						
						
					}
				} 

				System.out.println("...got resutls!");
				
				qexec.close();
				
				System.out.println("Got " + tc + " properties");

				if(tc<this.limit)
				{
					loop = false; //no more results to come, next type
					break;
				}
				
				
				sleep();
				
				//update limits
				startQuery = startQuery + this.limit;		
				System.out.println("Looping " + startQuery);
			}
		}
		
		
		return true;
	}
	

//	public Map<String, Map<String, Integer>> getOutgoingProps() {
//		return outgoingProps;
//	}
//
//	public Map<String, Map<String, Integer>> getIngoingProps() {
//		return ingoingProps;
//	}
//
//
//	public void setOutgoingProps(Map<String, Map<String, Integer>> outgoingProps) {
//		this.outgoingProps = outgoingProps;
//	}
//
//	public void setIngoingProps(Map<String, Map<String, Integer>> ingoingProps) {
//		this.ingoingProps = ingoingProps;
//	}
	
	private String stringPropertiesCount(Map<String, Map<String, Integer>> countprops)
	{
		String res = "";

    	Iterator oit = countprops.entrySet().iterator();
        while (oit.hasNext()) {
            Map.Entry pair = (Map.Entry)oit.next();
            String type = (String) pair.getKey();
            Map<String, Integer> pc = (Map<String, Integer>) pair.getValue();
            res+=type+":\n";

        	Iterator oit2 = pc.entrySet().iterator();
            while (oit2.hasNext()) {
                Map.Entry pair2 = (Map.Entry)oit2.next();
                String prop = (String) pair2.getKey();
                Integer count = (Integer) pair2.getValue();
                res+=("--"+prop+"="+count+"\n");
            }    
        }
        return res;
	}
	
//	public String stringOutgoingProperties()
//	{
//		return this.stringPropertiesCount(this.outgoingProps);
//	}
//	
//	public String stringIngoingProperties()
//	{
//		return this.stringPropertiesCount(this.ingoingProps);
//	}

	public void toFilePropertiesCount(String path, String plist)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(plist);
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	public void toFileIngoingProperties(String path)
//	{
//		this.toFilePropertiesCount(path, this.stringPropertiesCount(ingoingProps));
//	}
//	
//
//	public void toFileOutgoingProperties(String path)
//	{
//		this.toFilePropertiesCount(path, this.stringPropertiesCount(outgoingProps));
//	}
	
	

}
