package oeg.upm.isantana.ldviz.sparql;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;

import oeg.upm.isantana.ldviz.utils.Counter;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;

public class RDFSLabels extends SPARQLSource {

	HashMap<String, List<String>> resourceLabels;
	private NotRelevantProperties nrp;

	
	public RDFSLabels(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp)
	{
		super(sparqlEndpointUrl, namedGraph);
		this.resourceLabels = new HashMap<String, List<String>>();
		this.nrp = notrp;
	}

	
	public RDFSLabels(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, int lim, int off, int w)
	{
		super(sparqlEndpointUrl, namedGraph, lim, off, w);
		this.resourceLabels = new HashMap<String, List<String>>();
		this.nrp = notrp;
	}

	public HashMap<String, List<String>> getResourceLabels() {
		return resourceLabels;
	}
	
	public List<String> getTypeList(String resuri)
	{
		if(!resourceLabels.containsKey(resuri))
			return new ArrayList<String>();
		
		
		return resourceLabels.get(resuri);
	}

	public void setResourceLabels(HashMap<String, List<String>> rlabs) {
		this.resourceLabels = rlabs;
	}
	
	public boolean resourceLabels()
	{
		retrieveResourceLabels();//so far not looping for this one, just pagination
		return true;
	}
	
	private boolean retrieveResourceLabels() 
	{
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
			
		
		String queryString =  "SELECT DISTINCT ?r  ?t \n" + fromClause + "\n"
							+ "	WHERE "
							+ "{ { SELECT DISTINCT ?r  ?t \n" 
							+ "WHERE { ?r <http://www.w3.org/2000/01/rdf-schema#label> ?t .}\n"
							+ "ORDER BY ?r ?t \n"
							+ "} } OFFSET @@START@@ LIMIT " + this.limit;


		Query query = QueryFactory.create();

		int startQuery = this.offset;
		
		boolean loop = true;	
		while (loop) {
			

			String paramQueryString = queryString.replace("@@START@@", String.valueOf(startQuery));
			System.out.println("QUERY:"+paramQueryString);


			QueryFactory.parse(query, paramQueryString, "", Syntax.syntaxSPARQL_11);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(this.sparqlEndpointUrl, query);
			ResultSet results = null;
			try
			{
				Counter.increase();
				results = qexec.execSelect(); 
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
			
			int tc = 0;
			while (results.hasNext()) {
				tc++;
				
				QuerySolution binding = results.nextSolution();
				Resource res = (Resource) binding.get("r");
				String label = binding.get("t").asLiteral().toString();

				//we don't have info for this resource
				//lets create a entry list
				if (!resourceLabels.containsKey(res.getURI())) {
					resourceLabels.put(res.getURI(), new ArrayList<String>());
				}

				//add the type to the list
				resourceLabels.get(res.getURI()).add(label);
			} 
			
			qexec.close();
			
			
			if(tc<this.limit)
			{
				loop = false; //no more results to come, end
				break;
			}
			
			sleep();
			
			//update limits
			startQuery = startQuery + this.limit;		
			System.out.println("Looping " + startQuery);
		}
		
		return true;
	}
	
	public String stringResourceLabels()
	{
		String r = "";
		StringBuilder res = new StringBuilder();
    	Iterator it = resourceLabels.entrySet().iterator();
    	while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          res.append(pair.getKey());
          res.append(": <");
          
          for(String t : (List<String>) pair.getValue())
          {
        	 res.append(t);
        	 res.append(",");
          }

          if(res.toString().contains(","))
        	  r=res.toString().substring(0,res.length()-1);
          else
        	  r= res.toString();
          
          r+=">\n";
    	}	
    	
    	return r;
	}
	
	public void toFileResourceLabels(String path)
	{

		try {
			PrintWriter out = new PrintWriter(path);
			out.println(this.stringResourceLabels());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
