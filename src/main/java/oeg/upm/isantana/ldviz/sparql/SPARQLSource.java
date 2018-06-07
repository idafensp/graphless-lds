package oeg.upm.isantana.ldviz.sparql;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class SPARQLSource {
	
	protected String sparqlEndpointUrl;
	protected String namedGraph;
	protected boolean pagination;
	protected int limit;
	protected int offset;
	protected int wait;
	

	protected HashMap<String, HashSet<String>> parsingErrors;
	

	public SPARQLSource(String sparqlEndpointUrl, String namedGraph) {
		super();
		this.sparqlEndpointUrl = sparqlEndpointUrl;
		this.namedGraph = namedGraph;
		this.parsingErrors = new HashMap<String, HashSet<String>>();

	}
	

	public SPARQLSource(String sparqlEndpointUrl, String namedGraph, int lim, int off, int w) {
		super();
		this.sparqlEndpointUrl = sparqlEndpointUrl;
		this.namedGraph = namedGraph;
		this.offset = off;
		this.limit = lim;
		this.wait = w;
		this.pagination = true;
		this.parsingErrors = new HashMap<String, HashSet<String>>();
	}
	
	public String getSparqlEndpointUrl() {
		return sparqlEndpointUrl;
	}
	public String getNamedGraph() {
		return namedGraph;
	}
	public void setSparqlEndpointUrl(String sparqlEndpointUrl) {
		this.sparqlEndpointUrl = sparqlEndpointUrl;
	}
	public void setNamedGraph(String namedGraph) {
		this.namedGraph = namedGraph;
	}

	public boolean isPagination() {
		return pagination;
	}

	public int getLimit() {
		return limit;
	}

	public int getOffset() {
		return offset;
	}

	public void setPagination(boolean pagination) {
		this.pagination = pagination;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}


	public int getWait() {
		return wait;
	}


	public void setWait(int wait) {
		this.wait = wait;
	}
	
	
	protected void sleep()
	{
		//wait the interval
		try
		{
			System.out.print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Waiting  " + this.wait +  " seconds...");
			Thread.sleep(this.wait*1000); // do nothing for N seconds
			System.out.println("... and go!");
		}
		catch(InterruptedException e)
		{
			e.printStackTrace();
		}
	}


	public HashMap<String, HashSet<String>> getParsingErrors() {
		return parsingErrors;
	}


	public void setParsingErrors(HashMap<String, HashSet<String>> parsingErrors) {
		this.parsingErrors = parsingErrors;
	}
	
	public void addParsingError(String key, String query)
	{
		if(!this.parsingErrors.containsKey(key))
		{
			this.parsingErrors.put(key, new HashSet<String>());
		}
		this.parsingErrors.get(key).add(query);
	}
	public String toStringParsingErrors()
	{
		StringBuilder res = new StringBuilder();
    	Iterator it = this.parsingErrors.entrySet().iterator();
    	

  	    System.out.print("\nStringing " + this.parsingErrors.entrySet().size() + " parsing exceptions (resources and queries)");
    	int step = this.parsingErrors.entrySet().size() / 100 +1;
    	int count = 0;
    	int perc = 0;
    	
    	while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          res.append("\n----Queries for: ");
          res.append(pair.getKey());
          res.append(" ---");
          
          for(String t : (HashSet<String>) pair.getValue())
          {
        	  res.append(t);
        	  res.append("\n");
          }
          res.append("---- // ----\n");
          
    	}
    	
    	return res.toString();
	}
	
	public void toFileParsingErrors(String path)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(this.toStringParsingErrors());
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
