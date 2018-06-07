package oeg.upm.isantana.ldviz.graph;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import oeg.upm.isantana.ldviz.graph.neo4j.BatchNode;
import oeg.upm.isantana.ldviz.graph.neo4j.BatchRel;
import oeg.upm.isantana.ldviz.graph.neo4j.CypherGraphHandler;
import oeg.upm.isantana.ldviz.sparql.RDFSLabels;
import oeg.upm.isantana.ldviz.sparql.RDFTypes;
import oeg.upm.isantana.ldviz.sparql.SPARQLSource;
import oeg.upm.isantana.ldviz.utils.Counter;
import oeg.upm.isantana.ldviz.utils.HashNodes;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;
import oeg.upm.isantana.ldviz.utils.ResourceProperties;

public class TriplesGraph extends SPARQLSource {

	
//  DEPRECATED SO FAR	
//	private ArrayList<String> nodeQ;
//	private ArrayList<String> propQ;

	private String graphName;
	
	private HashSet<String> allSubjects;

	
	private ArrayList<BatchNode> nodeBatch;
	private ArrayList<BatchRel> propBatch;

	private Map<String, Integer> resIngoingDegree;
	private Map<String, Integer> resOutgoingDegree;
	
	private int maxIngoingDegree;
	private int maxOutogingDegree;
	//private int maxDegree;
	
	private NotRelevantProperties nrp;

	private HashSet<String> createdNodes;
	private HashSet<String> notFoundOutResources;
	private HashSet<String> notFoundIngResources;
	
	private ResourceProperties rp;
	private CypherGraphHandler cgh;
	private RDFTypes rdft;
	private RDFSLabels rdfslabels;
	
	private Driver neodriver;
	private int countLiterals;
	
	
	public TriplesGraph(String gn, String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, ResourceProperties r, 
			CypherGraphHandler cypgh, RDFTypes rdftypes, RDFSLabels rdfslab, String url, String user, String pass) {
		super(sparqlEndpointUrl, namedGraph);

//		this.nodeQ = new ArrayList<String>();
//		this.propQ = new ArrayList<String>();

		this.allSubjects = new HashSet<String>();
		
		this.graphName = gn;

		this.nodeBatch = new ArrayList<BatchNode>();
		this.propBatch = new ArrayList<BatchRel>();
		
		this.nrp = notrp;
		this.rp = r;
		this.rdft = rdftypes;
		this.rdfslabels =rdfslab;
		this.cgh = cypgh;
		
		this.neodriver = GraphDatabase.driver(url, AuthTokens.basic(user, pass));

		this.createdNodes = new HashSet<String>();
		this.notFoundIngResources = new HashSet<String>();
		this.notFoundOutResources = new HashSet<String>();

		this.resIngoingDegree = new HashMap<String, Integer>();
		this.resOutgoingDegree = new HashMap<String, Integer>();
		
		//this.maxDegree = -1;
		this.maxIngoingDegree = -1;
		this.maxOutogingDegree = -1;
		
	}

	
	public TriplesGraph(String gn, String sparqlEndpointUrl, String namedGraph, NotRelevantProperties notrp, ResourceProperties r, CypherGraphHandler cypgh, 
			RDFTypes rdftypes, RDFSLabels rdfslab,  String url, String user, String pass, int lim, int off, int w) {
		super(sparqlEndpointUrl, namedGraph, lim, off, w);

//		this.nodeQ = new ArrayList<String>();
//		this.propQ = new ArrayList<String>();
		
		this.allSubjects = new HashSet<String>();

		this.graphName = gn;
		
		this.nodeBatch = new ArrayList<BatchNode>();
		this.propBatch = new ArrayList<BatchRel>();
		
		this.nrp = notrp;
		this.rp = r;
		this.rdft = rdftypes;
		this.rdfslabels =rdfslab;
		this.cgh = cypgh;
		
		this.neodriver = GraphDatabase.driver(url, AuthTokens.basic(user, pass));

		this.createdNodes = new HashSet<String>();
		this.notFoundIngResources = new HashSet<String>();
		this.notFoundOutResources = new HashSet<String>();

		this.resIngoingDegree = new HashMap<String, Integer>();
		this.resOutgoingDegree = new HashMap<String, Integer>();

		//this.maxDegree = -1;
		this.maxIngoingDegree = -1;
		this.maxOutogingDegree = -1;
	}

	private boolean retrieveAllSubjects() 
	{
		
		System.out.println("Starting retrieveAllSubjects()");
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
			
		
		
		String queryString = "SELECT ?s\n" + fromClause + "\n"
				+ "WHERE {\n"
				+ "SELECT distinct ?s\n"
				+ "WHERE { ?s ?p ?o .} \n"
				+ "ORDER by ?s \n"
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
				Resource type = (Resource) binding.get("s");

				allSubjects.add(type.getURI());	
				tc++;
			} 
			
			System.out.println("Got " + tc + " subjects - Total " + allSubjects.size());
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
	
	public boolean generateAllTriples() throws NoSuchAlgorithmException
	{
		if(!this.retrieveAllSubjects())
			return false;
		
		
		System.out.println("Starting retrieveAllTriples()");
		

		HashSet<String> irrels = new HashSet<String>();

		countLiterals = 0;
		
		
		String fromClause = "";
		if(!this.namedGraph.isEmpty())
			fromClause = "from <" + this.namedGraph + ">";
		
		int currentsub = 1;
		int totalsub = this.allSubjects.size();
		
		//for each type, ask for its resources and populate the list
		for(String suburi : this.allSubjects)
		{

			String queryString = "SELECT ?p ?o\n" + fromClause + "\n"
					+ "WHERE {\n"
					+ "SELECT distinct ?p ?o \n"
					+ "WHERE { <"+suburi+"> ?p ?o.}\n"
					+ "ORDER BY ?p ?o\n"
				    + "} OFFSET @@START@@ LIMIT "  + this.limit;

			Query query = QueryFactory.create();

			int startQuery = this.offset;
			
			boolean loop = true;	
			while (loop) {
				

				String paramQueryString = queryString.replace("@@START@@", String.valueOf(startQuery));
				System.out.println(currentsub++ + "/" + totalsub + "->QUERY:"+paramQueryString);


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
					loop = false;
					break;
				}
				
				int tc = 0;
				int itc = 0;
				while (results.hasNext()) {
					
					tc++;

					QuerySolution binding = results.nextSolution();
					Resource prop = (Resource) binding.get("p");
					RDFNode obj = (RDFNode) binding.get("o");

					//TODO what to do with multiple labels?

					String slab = this.getLabel(suburi);
					String plab = this.getLabel(prop);
					String olab = "";
					
					if(obj.isResource())
						 olab = this.getLabel((Resource) obj);

					//if the prop is irrelevant, we do not consider this triple at all
					if (nrp.irrelevant(prop.getURI())) {
						itc++;
						irrels.add(prop.getURI());
						continue;
					}

					if (!createdNodes.contains(suburi)) {
						//create a node for each subject
						//nodeQ.add(cgh.generateCypherNodeQuery(sub.getURI(), slab, rdft.getTypeList(sub.getURI())));

						//add batch node
						nodeBatch.add(cgh.generateBatchNode(suburi, slab, rdft.getTypeList(suburi)));

						createdNodes.add(suburi);
					}

					//add everytime we found it as subject
					addOutgoingDegree(HashNodes.hashNodeUri(suburi, this.graphName));

					//create a node for each object
					//check that is no literal
					//what happens if the object is a literal?		

					String objectNodeId = "";
					if (!obj.isLiteral()) {

						objectNodeId = HashNodes.hashNodeUri(obj.asResource().getURI(), this.graphName);
						if (!createdNodes.contains(obj.asResource().getURI())) {
							olab = obj.asResource().getLocalName();
							
							//add batch node
							nodeBatch.add(cgh.generateBatchNode(obj.asResource().getURI(), olab,
									rdft.getTypeList(obj.asResource().getURI())));

							createdNodes.add(obj.asResource().getURI());
						}

						//add everytime we found it as object
						addIngoingDegree(HashNodes.hashNodeUri(obj.asResource().getURI(), this.graphName));
					} else {
						//in case we find a litereal, we generate the id combining the subject and the literal value
						objectNodeId = HashNodes.hashNodeUri(suburi + obj.asLiteral().toString(), this.graphName);

						//add cypher query
						//nodeQ.add(cgh.generateCypherNodeQueryLiteral(litNodeId, obj.asLiteral().toString()));

						//add batch node
						nodeBatch.add(cgh.generateBatchLiteralNode(objectNodeId, obj.asLiteral().toString()));

						createdNodes.add(obj.asLiteral().toString());

						countLiterals++;
					}

					//what happens if the object is a literal?

					double outw = rp.getNomalizedOutgoingWeight(suburi, prop.getURI());
					if (outw == -1) {
						this.notFoundOutResources.add(suburi);
					}

					String subNodeId = HashNodes.hashNodeUri(suburi, this.graphName);

					
					System.out.println(">>>GENERATING DIRECT BREL:" + prop.getURI() + ", subNodeId=" + subNodeId
										+ ", objectNodeId=" + objectNodeId
										+ ", plab=" + plab +  ", outw=" + outw);
					//add batch relations outgoing
					propBatch.add(
							cgh.generateBatchRelationship(prop.getURI(), subNodeId, objectNodeId, plab, outw, "direct"));

					if (!obj.isLiteral()) {
						double ingw = rp.getNomalizedIngoingWeight(obj.asResource().getURI(), prop.getURI());
						if (ingw == -1) {
							this.notFoundIngResources.add(suburi);
						}
						

						System.out.println("<<<GENERATING INDIRECT BREL:" + prop.getURI() + ", subNodeId=" + subNodeId + ", objectNodeId=" + objectNodeId
											+ ", plab=" + plab +  ", ingw=" + ingw);

						//add batch relations ingoing only if it is not a literal
						propBatch.add(cgh.generateBatchRelationship(prop.getURI(), objectNodeId, subNodeId, plab, ingw,
								"inverse"));
					}

				} //end result.hasNext() loop 
				
				System.out.println("Got [irrel=" + itc + "/" + tc + "] triples for type " + suburi );
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
		

		
		System.out.println("Found irrelevant properties");
		for(String i: irrels)
		{
			System.out.println(i+"; ");
		}
		
		
		return true;
	}
	
	
	
	
	private void executeCypherQueries(List<String> queries)
	{
    	
		Session session = this.neodriver.session();
		
		for(String q : queries)
		{
			session.run(q);
		}
		
    	session.close();
	}


	public void printNotFoundIngResources()
	{
		System.out.println("Not found ingoing:");
		for(String n : this.notFoundIngResources)
		{
			System.out.println(n);
		}
	}



	public void printNotFoundOutResources()
	{
		System.out.println("Not found outgoing:");
		for(String n : this.notFoundOutResources)
		{
			System.out.println(n);
		}
	}


	public int getCountLiterals() {
		return countLiterals;
	}


	public void setCountLiterals(int countLiterals) {
		this.countLiterals = countLiterals;
	}


	public ArrayList<BatchNode> getNodeBatch() {
		return nodeBatch;
	}


	public ArrayList<BatchRel> getPropBatch() {
		return propBatch;
	}


	public void setNodeBatch(ArrayList<BatchNode> nodeBatch) {
		this.nodeBatch = nodeBatch;
	}


	public void setPropBatch(ArrayList<BatchRel> propBatch) {
		this.propBatch = propBatch;
	}


	public HashSet<String> getCreatedNodes() {
		return createdNodes;
	}


	public HashSet<String> getNotFoundOutResources() {
		return notFoundOutResources;
	}


	public HashSet<String> getNotFoundIngResources() {
		return notFoundIngResources;
	}


	public void setCreatedNodes(HashSet<String> createdNodes) {
		this.createdNodes = createdNodes;
	}


	public void setNotFoundOutResources(HashSet<String> notFoundOutResources) {
		this.notFoundOutResources = notFoundOutResources;
	}


	public void setNotFoundIngResources(HashSet<String> notFoundIngResources) {
		this.notFoundIngResources = notFoundIngResources;
	}

	private void addIngoingDegree(String uri)
	{
		int nd = addDegree(1, uri, this.resIngoingDegree);
		if( nd > this.maxIngoingDegree)
			this.maxIngoingDegree = nd;
	}
	private void addOutgoingDegree(String uri)
	{
		int nd = addDegree(1, uri, this.resOutgoingDegree);
		if(nd > this.maxOutogingDegree)
			this.maxOutogingDegree = nd;
	}
	
	private int addDegree(int d, String uri, Map<String, Integer> map)
	{
		if(!map.containsKey(uri))
		{
			map.put(uri, 0);
		}
		
		int cd = map.get(uri) + d;
		map.put(uri, cd);
		
		return cd;
	}



	public Map<String, Integer> getResIngoingDegree() {
		return resIngoingDegree;
	}


	public Map<String, Integer> getResOutgoingDegree() {
		return resOutgoingDegree;
	}


	public void setResIngoingDegree(Map<String, Integer> resIngoingDegree) {
		this.resIngoingDegree = resIngoingDegree;
	}


	public void setResOutgoingDegree(Map<String, Integer> resOutgoingDegree) {
		this.resOutgoingDegree = resOutgoingDegree;
	}
	
	public int getMaxIngoingDegree() {
		return maxIngoingDegree;
	}


	public int getMaxOutogingDegree() {
		return maxOutogingDegree;
	}


	public void setMaxIngoingDegree(int maxIngoingDegree) {
		this.maxIngoingDegree = maxIngoingDegree;
	}


	public void setMaxOutogingDegree(int maxOutogingDegree) {
		this.maxOutogingDegree = maxOutogingDegree;
	}


	//get label from
	// rdfslabels
	//   if not: after / and #
	//     if not: last 15 chars
	private String getLabel(Resource res)
	{
		System.out.print("Getting label for resource " + res.getURI());
		try {
			if (rdfslabels.getResourceLabels().containsKey(res.getURI()))
				return rdfslabels.getResourceLabels().get(res.getURI()).get(0); //TODO now we are  getting only the first 
			else {
				if ((res.getLocalName() != null) && (!res.getLocalName().isEmpty())) {
					System.out.println(" is localname " + res.getLocalName());
					return res.getLocalName();
				} else {
					String uri = res.getURI();
					if (uri.indexOf("/") > 0) {
						uri = uri.substring(uri.lastIndexOf("/")+1, uri.length());
						System.out.print(" is / ");

						if (uri.indexOf("#") > 0) {
							uri = uri.substring(uri.lastIndexOf("#")+1);
							System.out.print(" is # ");
						}	
						
						if(uri.length()>1)
						{	
							System.out.println(uri);
							return uri;
						}
						else
						{
							System.out.print(" not long ");
							int len = res.getURI().length()-15;
							if(len < 0)
							{
								len = 0;
							}
							System.out.println("..."+res.getURI().toString().substring(len));
							return "..."+res.getURI().toString().substring(len);
						}
					}
					else 
					{
						if (uri.indexOf("#") > 0) {
							uri = uri.substring(uri.lastIndexOf("#")+1);
							System.out.print("is # ");
							if(uri.length()>1)
							{
								System.out.println(uri);
								return uri;
							}
						}
						else
						{
							System.out.print(" is not long ");
							int len = res.getURI().length()-15;
							if(len < 0)
							{
								len = 0;
							}
							System.out.println("..."+res.getURI().toString().substring(len));
							return "..."+res.getURI().toString().substring(len);
						}
					}
					
					return uri;
				}
			} 
		} catch (Exception e) {
			System.err.println("Error while getting label for resource" + res.toString());
			e.printStackTrace();
		}
		System.out.print(" is not long ");
		int len = res.getURI().length()-15;
		if(len < 0)
		{
			len = 0;
		}
		System.out.println("..."+res.getURI().toString().substring(len));
		return "..."+res.getURI().toString().substring(len);
	}
	
	private String getLabel(String uri)
	{

		System.out.print("Getting label for uri " + uri  + " - ");
		try {
			Model model = ModelFactory.createDefaultModel();
			Resource res = model.createResource(uri);
			return getLabel(res);
		} catch (Exception e) {
			System.err.println("Error while getting label for uri " + uri);
			e.printStackTrace();
		}							System.out.print(" is not long ");
		
		int len = uri.length()-15;
		if(len < 0)
		{
			len = 0;
		}
		System.out.println("..."+uri.toString().substring(len));
		return "..."+uri.toString().substring(len);
	}
}
