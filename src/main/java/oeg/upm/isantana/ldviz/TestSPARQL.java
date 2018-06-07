package oeg.upm.isantana.ldviz;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import oeg.upm.isantana.ldviz.graph.TriplesGraph;
import oeg.upm.isantana.ldviz.graph.neo4j.CypherGraphHandler;
import oeg.upm.isantana.ldviz.graph.neo4j.TripleGraphBatch;
import oeg.upm.isantana.ldviz.sparql.RDFTypes;
import oeg.upm.isantana.ldviz.sparql.TypesPropertyCount;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;
import oeg.upm.isantana.ldviz.utils.ResourceFileWriter;
import oeg.upm.isantana.ldviz.utils.ResourceProperties;

/**
 * Hello world!
 *
 */
public class TestSPARQL 
{
    public static void main( String[] args )
    {

//    	//String sparqlEP = "http://localhost:8890/sparql";
//    	String sparqlEP = args[0];
//    	String nrpPath = "";
//    	
//    	String graphName = args[1];
//    	String basePath = args[2] + graphName;
//    	String namedGraph = args[3];
//
//    	int limit = Integer.valueOf(args[4]);
//    	int offset = Integer.valueOf(args[5]);
//    	int wait = Integer.valueOf(args[6]);
//    	
//    	NotRelevantProperties nrp = new NotRelevantProperties(nrpPath);
//
//    	
// 
//    	TypesPropertyCount tp = new TypesPropertyCount(sparqlEP, namedGraph, nrp);
//    	
//    	RDFTypes rdft = new RDFTypes(sparqlEP, namedGraph, nrp, limit, offset, wait);
//   
//    	ResourceProperties rp = new ResourceProperties(nrp);
//    	
//
//    	CypherGraphHandler cgh = new CypherGraphHandler(graphName);
//    	TriplesGraph tg = new TriplesGraph(graphName,sparqlEP, namedGraph, nrp, rp, cgh, rdft,"bolt://localhost:7687","neo4j","12345", limit, offset, wait);
//    	
//    	try {
//
//    		//generate the queries and batch nodes
//    		if(!tg.generateCypherQueriesAndBatch())
//    		{
//    			System.err.println("ERROR: error while generating the queries and batch");
//    			return;
//    		}
//
//			System.out.println("\n+++++++++++FOUND LITERALS=" + tg.getCountLiterals());
//
//			System.out.println("\n+++++++++++NOT FOUND ING="+ tg.getNotFoundIngResources().size());
//			System.out.println("\n+++++++++++NOT FOUND OUT="+ tg.getNotFoundOutResources().size());
//
//			ResourceFileWriter.listToFile(basePath+"_notfound_ingoing.txt", tg.getNotFoundIngResources());
//			ResourceFileWriter.listToFile(basePath+"_notfound_outgoing.txt", tg.getNotFoundOutResources());
//			
////
////			System.out.println("\n+++++++++++WRITING "+tg.getNodeQ().size()+" NODES" );
////			ResourceFileWriter.listToFile(basePath+"_queries_nodes.txt", tg.getNodeQ());
////
////			System.out.println("\n+++++++++++WRITING "+tg.getPropQ().size()+" RELATIONS");
////			ResourceFileWriter.listToFile(basePath+"_queries_props.txt", tg.getPropQ());
//
//			
//
//			System.out.println("\n+++++++++++CREATING QUERY NODES" );
//			//tg.executeNodeQueries();
//			
//			System.out.println("\n+++++++++++CREATING QUERY PROPS" );
//			//tg.executePropQueries();
//			
//			TripleGraphBatch tgbatch = new TripleGraphBatch(graphName, tg.getNodeBatch(), tg.getPropBatch(),
//															tg.getResIngoingDegree(),tg.getResOutgoingDegree());
//			
//			System.out.println("\n+++++++++++BATCH NODES" );
//			tgbatch.runBatchInsertions(basePath+"/graph.db");
//			
//			
//			
//		} catch (NoSuchAlgorithmException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	
//		System.out.println("\n+++++++++++DONE! :)" );
    	

		String queryString = "select ?s ?p ?o " 
				+ "\n where " + 
				"\n{ {" + "  SELECT ?s ?p ?o \n" 
				+ "WHERE {"
				+ " ?s ?p ?o . "
				+ "}\n" 
				+ "ORDER BY ?s ?p ?o \n"
				+ "} } OFFSET 0 LIMIT 5000";
		
		System.out.println(queryString);
    	
    }
}
