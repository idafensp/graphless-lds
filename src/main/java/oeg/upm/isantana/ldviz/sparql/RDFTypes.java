package oeg.upm.isantana.ldviz.sparql;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import oeg.upm.isantana.ldviz.utils.HashNodes;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;

public class RDFTypes extends SPARQLSource {

	private HashMap<String, HashSet<String>> resourceTypes;
	private HashSet<String> allTypes;
	private HashSet<String> allResources;
	
	private NotRelevantProperties nrp;
	
	private String flushPath;
	
	
	public RDFTypes(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, String fp)
	{
		super(sparqlEndpointUrl, namedGraph);
		this.allTypes = new HashSet<String>();
		this.allResources = new HashSet<String>();
		this.resourceTypes = new HashMap<String, HashSet<String>>();
		this.flushPath = fp;
		this.nrp = notrp;
	}

	
	public RDFTypes(String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, int lim, int off, int w, String fp)
	{
		super(sparqlEndpointUrl, namedGraph, lim, off, w);
		this.resourceTypes = new HashMap<String, HashSet<String>>();
		this.allTypes = new HashSet<String>();
		this.allResources = new HashSet<String>();
		this.flushPath = fp;
		this.nrp = notrp;
	}

	public HashMap<String, HashSet<String>> getResourceTypes() {
		return resourceTypes;
	}
	
	public HashSet<String> getTypeList(String resuri)
	{
		if(!resourceTypes.containsKey(resuri))
			return new HashSet<String>();
		
		
		return resourceTypes.get(resuri);
	}

	public void setResourceTypes(HashMap<String, HashSet<String>> resourceTypes) {
		this.resourceTypes = resourceTypes;
	}
	
	
	public boolean retrieveTypeResources()
	{
		System.out.println("Starting retrieveTypeResources()");
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		//for each type, ask for its resources and populate the list
		int counttypes =0;
		for(String turi : this.allTypes)
		{
			System.out.println("########### Types ############");
			System.out.println("# " +  counttypes++ + "/" +  this.allTypes.size());
			System.out.println("##############################");

			String queryString = "SELECT ?r\n" + fromClause + "\n"
					+ "WHERE {\n"
					+ "SELECT distinct ?r\n"
					+ "WHERE { ?r a <"+turi+"> .}\n"
					+ "ORDER BY ?r\n"
				    + "} OFFSET @@START@@ LIMIT "  + this.limit;

			Query query = QueryFactory.create();

			int startQuery = this.offset;
			
			boolean loop = true;	
			while (loop) {
				

				String paramQueryString = queryString.replace("@@START@@", String.valueOf(startQuery));
				System.out.println("QUERY:"+paramQueryString);


				try {
					QueryFactory.parse(query, paramQueryString, "", Syntax.syntaxSPARQL_11);
				} catch (org.apache.jena.query.QueryParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					addParsingError(turi,paramQueryString);
					continue;
				} catch (Exception e1) {
					addParsingError(turi,paramQueryString);
					e1.printStackTrace();
					continue;
				}
				
				
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
					loop = false;
					break;
				}
				
				int tc = 0;
				while (results.hasNext()) 
				{
					QuerySolution binding = results.nextSolution();
					Resource res = (Resource) binding.get("r");
					
					if(!resourceTypes.containsKey(res.getURI()))
					{
						resourceTypes.put(res.getURI(), new HashSet<String>());
					}
					
					resourceTypes.get(res.getURI()).add(turi);
					tc++;
				} 
				
				System.out.println("Got " + tc + " resources for type " + turi);
				qexec.close();
				
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
	

	
	public boolean retrieveTypeResourcesLocal()
	{
		System.out.println("Starting retrieveTypeResources()");
		
		HashSet<String> localResourceTypes = new HashSet<String>();

		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		//for each type, ask for its resources and populate the list
		int counttypes =0;
		for(String turi : this.allTypes)
		{
			System.out.println("########### Types ############");
			System.out.println("# " +  counttypes++ + "/" +  this.allTypes.size());
			System.out.println("##############################");

			String queryString = "SELECT ?r\n" + fromClause + "\n"
					+ "WHERE {\n"
					+ "SELECT distinct ?r\n"
					+ "WHERE { ?r a <"+turi+"> .}\n"
					+ "ORDER BY ?r\n"
				    + "} OFFSET @@START@@ LIMIT "  + this.limit;

			Query query = QueryFactory.create();

			int startQuery = this.offset;
			
			boolean loop = true;	
			while (loop) {
				

				String paramQueryString = queryString.replace("@@START@@", String.valueOf(startQuery));
				System.out.println("QUERY:"+paramQueryString);


				try {
					QueryFactory.parse(query, paramQueryString, "", Syntax.syntaxSPARQL_11);
				} catch (org.apache.jena.query.QueryParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					addParsingError(turi,paramQueryString);
					continue;
				} catch (Exception e1) {
					addParsingError(turi,paramQueryString);
					e1.printStackTrace();
					continue;
				}
				
				
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
					loop = false;

					flushResourcesToDisk(turi,localResourceTypes);
					localResourceTypes = null;
					System.gc();
					
					break;
				}
				
				int tc = 0;
				while (results.hasNext()) 
				{
					QuerySolution binding = results.nextSolution();
					Resource res = (Resource) binding.get("r");
					
					
					localResourceTypes.add(turi);
					tc++;
				} 
				
				System.out.println("Got " + tc + " resources for type " + turi);
				qexec.close();
				
				if(tc<this.limit)
				{
					loop = false; //no more results to come, next type
					flushResourcesToDisk(turi,localResourceTypes);
					localResourceTypes = null;
					System.gc();
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
	
	private void flushResourcesToDisk(String uri, HashSet<String> lrt)
	{
		try {
			String id=HashNodes.hashNodeUri(uri, "FLUSH");
			System.out.println("flushing: "+flushPath+id);
			
			this.toFile(flushPath+id, this.stringResourceTypesLocal(uri, lrt));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public boolean retrieveResourceTypes()
	{
		System.out.println("Starting retrieveResourceTypes()");
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		int countres = 0;
		//for each type, ask for its resources and populate the list
		for(String resuri : this.allResources)
		{


			System.out.println("######### Resources ##########");
			System.out.println("# " +  countres++ + "/" +  this.allResources.size());
			System.out.println("##############################");
			
			String queryString = "SELECT ?t\n" + fromClause + "\n"
					+ "WHERE {\n"
					+ "SELECT distinct ?t\n"
					+ "WHERE { <"+resuri+"> a ?t .}\n"
					+ "ORDER BY ?t\n"
				    + "} OFFSET @@START@@ LIMIT "  + this.limit;

			Query query = QueryFactory.create();

			int startQuery = this.offset;
			
			boolean loop = true;	
			while (loop) {
				

				String paramQueryString = queryString.replace("@@START@@", String.valueOf(startQuery));
				System.out.println("QUERY:"+paramQueryString);


				try {
					QueryFactory.parse(query, paramQueryString, "", Syntax.syntaxSPARQL_11);
				} catch (org.apache.jena.query.QueryParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					addParsingError(resuri,paramQueryString);
					continue;
				} catch (Exception e1) {
					addParsingError(resuri,paramQueryString);
					e1.printStackTrace();
					continue;
				}
				
				
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
					loop = false;
					break;
				}
				
				int tc = 0;

				if(!resourceTypes.containsKey(resuri))
				{
					resourceTypes.put(resuri, new HashSet<String>());
				}
				
				while (results.hasNext()) 
				{
					QuerySolution binding = results.nextSolution();
					Resource restype = (Resource) binding.get("t");
					
					
					resourceTypes.get(resuri).add(restype.getURI());
					tc++;
				} 
				
				System.out.println("Got " + tc + " types for type " + resuri);
				qexec.close();
				
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
	
	public boolean retrieveAllTypes() 
	{
		
		System.out.println("Starting retrieveAllTypes()");
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
			
		
		String queryString = "SELECT distinct ?t\n" + fromClause + "\n"
				+ "WHERE {\n"
				+ "SELECT distinct ?t\n"
				+ "WHERE { ?r a ?t .} \n"
				+ "ORDER by ?t \n"
				+ "} OFFSET @@START@@ LIMIT "  + this.limit;


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
			while (results.hasNext()) 
			{
				QuerySolution binding = results.nextSolution();
				Resource type = (Resource) binding.get("t");

				allTypes.add(type.getURI());	
				tc++;
			} 
			
			System.out.println("Got " + tc + " types - Total " + allTypes.size());
			qexec.close();

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
		
		return true;
	}
	
	
	//all *typed* resources
	public boolean retrieveAllResources() 
	{
		
		System.out.println("Starting retrieveAllResources()");
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">\n";
			
		
		String queryString = "SELECT ?r\n" + fromClause 
				+ "WHERE {\n"
				+ "SELECT distinct ?r\n"
				+ "WHERE { ?r a ?t .} \n"
				+ "ORDER by ?r \n"
				+ "} OFFSET @@START@@ LIMIT "  + this.limit;


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
			while (results.hasNext()) 
			{
				QuerySolution binding = results.nextSolution();
				Resource resource = (Resource) binding.get("r");

				allResources.add(resource.getURI());	
				tc++;
			} 
			
			System.out.println("Got " + tc + " resources - Total " + allResources.size());
			qexec.close();

			if(tc<this.limit)
			{
				loop = false; //no more results to come, next resource
				break;
			}
			
			sleep();
			
			//update limits
			startQuery = startQuery + this.limit;		
			System.out.println("Looping " + startQuery);
		}
		
		return true;
	}
	
	public String stringResourceTypes()
	{
		StringBuilder res = new StringBuilder();
    	Iterator it = resourceTypes.entrySet().iterator();
    	

  	    System.out.print("String " + resourceTypes.entrySet().size() + " resources");
    	int step = resourceTypes.entrySet().size() / 100 +1;
    	int count = 0;
    	int perc = 0;
    	
    	while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          res.append(pair.getKey());
          res.append(": <");
          
          for(String t : (HashSet<String>) pair.getValue())
          {
        	  res.append(t);
        	  res.append(",");
          }
          
          
          res.append(">\n");
          if(count++%step == 0)
        	  System.out.print(perc++ + "%...");
    	}
    	
    	return res.toString();
	}
	
	public String stringResourceTypesLocal(String uri, HashSet<String> lrt)
	{
		StringBuilder res = new StringBuilder();
    	

  	    System.out.print("LOCAL string " + lrt.size() + " resources types to file");
    	int step = lrt.size() / 100 +1;
    	int count = 0;
    	int perc = 0;

  	    res.append(uri);
  	    res.append(":");
    	for(String t : lrt) {
          

          res.append(t);
          res.append(",");
          
          if(count++%step == 0)
        	  System.out.print(perc++ + "%...");
    	}
    	
    	return res.toString();
	}
	

	
	public String stringAllTypes()
	{
		StringBuilder res = new StringBuilder();

  	    System.out.print("String " + allTypes.size() + " alltypes to file");
    	int step = allTypes.size() / 100 +1;
    	int count = 0;
    	int perc = 0;
    	
		for(String turi : allTypes)
		{
      	  res.append(turi);
      	  res.append("\n");
      	
      	  if(count++%step == 0)
        	  System.out.print(perc++ + "%...");
    	
		}
    	
    	return res.toString();
	}
	
	public String stringAllResources()
	{
		StringBuilder res = new StringBuilder();

  	    System.out.print("String " + allResources.size() + " allResources to file");
    	int step = allResources.size() / 100 +1;
    	int count = 0;
    	int perc = 0;
    	
		for(String turi : allResources)
		{
      	  res.append(turi);
      	  res.append("\n");
      	
      	  if(count++%step == 0)
        	  System.out.print(perc++ + "%...");
    	
		}
    	
    	return res.toString();
	}
	
	public void toFileAllTypes(String path)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(this.stringAllTypes());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void toFile(String path, String content)
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
	
	public void toFileAllResources(String path)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(this.stringAllResources());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void toFileResourceTypes(String path)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(this.stringResourceTypes());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public HashSet<String> getAllTypes() {
		return allTypes;
	}


	public NotRelevantProperties getNrp() {
		return nrp;
	}


	public void setAllTypes(HashSet<String> allTypes) {
		this.allTypes = allTypes;
	}


	public void setNrp(NotRelevantProperties nrp) {
		this.nrp = nrp;
	}

}
