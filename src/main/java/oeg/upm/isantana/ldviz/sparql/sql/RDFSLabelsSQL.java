package oeg.upm.isantana.ldviz.sparql.sql;

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

import oeg.upm.isantana.ldviz.sparql.SPARQLSource;
import oeg.upm.isantana.ldviz.utils.Counter;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;

public class RDFSLabelsSQL extends SPARQLSource {

	//HashMap<String, List<String>> resourceLabels;
	private NotRelevantProperties nrp;

	private SQLHelper sqlh;

	
	public RDFSLabelsSQL(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, String graphName)
	{
		super(sparqlEndpointUrl, namedGraph);
		//this.resourceLabels = new HashMap<String, List<String>>();
		this.nrp = notrp;
		sqlh = new SQLHelper(graphName);

	}

	
	public RDFSLabelsSQL(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, int lim, int off, int w, String graphName)
	{
		super(sparqlEndpointUrl, namedGraph, lim, off, w);
		//this.resourceLabels = new HashMap<String, List<String>>();
		this.nrp = notrp;
		sqlh = new SQLHelper(graphName);

	}

	
	public boolean resourceLabels()
	{
		sqlh.createTableRLabels(SQLConstants.RES_LABELS);
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
				String lang = binding.get("t").asLiteral().getLanguage();
				
				//found label, insert in SQL
				sqlh.insertResLabel(SQLConstants.RES_LABELS, res.getURI(), label, lang);
				
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
	 
	

}
