package oeg.upm.isantana.ldviz.sql;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.jena.rdf.model.Model;

import oeg.upm.isantana.ldviz.graph.TriplesGraph;
import oeg.upm.isantana.ldviz.graph.neo4j.BatchNode;
import oeg.upm.isantana.ldviz.graph.neo4j.BatchRel;
import oeg.upm.isantana.ldviz.graph.neo4j.CypherGraphHandler;
import oeg.upm.isantana.ldviz.graph.neo4j.TripleGraphBatch;
import oeg.upm.isantana.ldviz.graph.sql.TriplesGraphSQL;
import oeg.upm.isantana.ldviz.sparql.RDFSLabels;
import oeg.upm.isantana.ldviz.sparql.RDFTypes;
import oeg.upm.isantana.ldviz.sparql.TypesPropertyCount;
import oeg.upm.isantana.ldviz.sparql.sql.RDFSLabelsSQL;
import oeg.upm.isantana.ldviz.sparql.sql.TypesPropertyCountSQL;
import oeg.upm.isantana.ldviz.utils.DataWriter;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;
import oeg.upm.isantana.ldviz.utils.ResourceFileWriter;
import oeg.upm.isantana.ldviz.utils.ResourceProperties;
import oeg.upm.isantana.ldviz.utils.sql.ResourcePropertiesSQL;

/**
 * Hello world!
 *
 */
public class AppSQL 
{
    public static void main( String[] args )
    {

    	System.out.println("*********************************");
    	System.out.println("*            GOING SQL          *");
    	System.out.println("*********************************");


    	// Get maximum size of heap in bytes. The heap cannot grow beyond this size.// Any attempt will result in an OutOfMemoryException.
    	long heapMaxSize = Runtime.getRuntime().maxMemory();
    	System.out.println("xmx="+ heapMaxSize);

    	//String sparqlEP = "http://localhost:8890/sparql";
    	String sparqlEP = args[0];
    	
    	
    	String nrpPath = "";
    	
    	String graphName = args[1];
    	
    	String basePath = args[2] + graphName;
    	String namedGraph = args[3];
    	if(!namedGraph.startsWith("http:"))
    		namedGraph ="";

    	int limit = Integer.valueOf(args[4]);
    	int offset = Integer.valueOf(args[5]);
    	int wait = Integer.valueOf(args[6]);
    	
    	boolean dbg = false; 
    	boolean wtf = false;
    	boolean insert = false;
    	int step = 0;
    	if(args.length > 7)
    	{
    		dbg= args[7].equals("true");
    		System.out.println("dbg:"+dbg);
        	if(args.length > 8)
        	{
        		wtf= args[8].equals("true");	
        		System.out.println("wtf:"+wtf);
            	if(args.length > 9)
            	{
            		insert= args[9].equals("true");	
            		System.out.println("insert:"+insert);
                	if(args.length > 10)
                	{
                		step= Integer.valueOf(args[10]);
                		System.out.println("step:"+step);
                		/*
                		 * always execute RDF types, no storing in SQL
                		 * 4 everything
                		 * 3 RDFSlabel and following
                		 * 2 resource property count and following
                		 * 1 only neo4j
                		 * 
                		 */
                	}
            	}
        	}
    	}
    	
    	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	Date startDate = new Date();
    	String startTime = dateFormat.format(startDate);
    	System.out.println("START:"+startTime); 
    	
    	NotRelevantProperties nrp = new NotRelevantProperties(nrpPath);
    	
    	System.out.println("\n+++++++++++START RDFTypes" );
    	
    	RDFTypes rdft = new RDFTypes(sparqlEP, namedGraph, nrp, limit, offset, wait, basePath+"/res/");
    	
		if(!rdft.retrieveAllTypes())
    	{
			System.err.println("ERROR: error while generating types of resources");
			return;
		}
		
		if(wtf) rdft.toFileAllTypes(basePath+"_all_types.txt");

		if(!rdft.retrieveTypeResources())
		{
			System.err.println("ERROR: error while generating types of resources");
			return;
		}

    	
    	System.out.println("Types:");
    	
    	//if(dbg)  System.out.println(rdft.stringResourceTypes());
    	if(wtf)  rdft.toFileResourceTypes(basePath+"_resource_types.txt"); 
    	if(wtf) rdft.toFileParsingErrors(basePath+"_resource_types_parsing_errors.txt");

    	TypesPropertyCountSQL tp = new TypesPropertyCountSQL(sparqlEP, namedGraph, nrp, rdft, limit, offset, wait, graphName);

    	if(step>3)
    	{
    	
	    	System.out.println("\n+++++++++++START TypesPropertyCount, step="+step );
	
	
	    	if(!tp.ingoingPropertiesCount())
	    	{
				System.err.println("ERROR: error while calculating ingoing properties");
				return;
			}
	    	
	    	if(!tp.outgoingPropertiesCount())
	    	{
				System.err.println("ERROR: error while calculating outgoing");
				return;
			}
	
	//    	System.out.println("Ingoing:");
	//    	if(dbg) System.out.println(tp.stringIngoingProperties());
	//    	if(wtf) tp.toFileIngoingProperties(basePath+"_ing_properties.txt"); 	
	//    	
	//    	System.out.println("Outgoing:");
	//    	if(dbg) System.out.println(tp.stringOutgoingProperties());
	//    	if(wtf) tp.toFileOutgoingProperties(basePath+"_out_properties.txt"); 	
	
	    	if(wtf) tp.toFileParsingErrors(basePath+"_properties_parsing_errors.txt"); 	
    	}
    	

    	RDFSLabelsSQL rdfslabels = new RDFSLabelsSQL(sparqlEP, namedGraph, nrp, limit, offset, wait,graphName);
    	
    	if(step>2)
    	{
	    	System.out.println("\n+++++++++++START RDFLabels, step =" + step );
	    	
	    	if(!rdfslabels.resourceLabels())
	    	{
				System.err.println("ERROR: error while generating the label of resources");
				//return;
			}
	    	
	    	System.out.println("Labels:");
	//    	if(dbg) System.out.println(rdfslabels.stringResourceLabels());
	//    	if(wtf) rdfslabels.toFileResourceLabels(basePath+"_resource_labels.txt");;
	//    	if(wtf) rdfslabels.toFileParsingErrors(basePath+"_labels_parsing_errors.txt"); 	
	    	
    	}

    	ResourcePropertiesSQL rp = new ResourcePropertiesSQL(nrp,graphName);

    	if(step>1)
    	{
    		System.out.println("\n+++++++++++START ResourceProperties & Normalizig, step=" +step);


        	System.out.println("Sum outgoing:");
        	rp.sumOutgoingProp();//tp.getOutgoingProps());
        	
        	
//        	if(dbg)  rp.printOutgoingProperties();
        	
        	System.out.println("Sum ingoing:");
        	rp.sumIngoingProp();// tp.getIngoingProps());    	      

//        	if(dbg)  rp.printIngoingProperties();
    	}
    	
    	
    	if(step>0)
    	{

        	System.out.println("\n+++++++++++START Graph Neo4j" );

        	CypherGraphHandler cgh = new CypherGraphHandler(graphName);
        
        	TriplesGraphSQL tg = new TriplesGraphSQL(graphName, sparqlEP, namedGraph, nrp, cgh, rdft, limit, offset, wait);
        	
        	try {

        		//generate the queries and batch nodes
        		if(!tg.generateAllTriples())
        		{
        			System.err.println("ERROR: error while generating the queries and batch");
        			return;
        		}
        		

    			System.out.println("\n+++++++++++FOUND LITERALS=" + tg.getCountLiterals());

    			System.out.println("\n+++++++++++NOT FOUND ING="+ tg.getNotFoundIngResources().size());
    			System.out.println("\n+++++++++++NOT FOUND OUT="+ tg.getNotFoundOutResources().size());
    			
    			

    	    	if(wtf) tg.toFileParsingErrors(basePath+"_triples_parsing_errors.txt"); 	

    			if(wtf) ResourceFileWriter.listToFile(basePath+"_notfound_ingoing.txt", tg.getNotFoundIngResources());
    			if(wtf) ResourceFileWriter.listToFile(basePath+"_notfound_outgoing.txt", tg.getNotFoundOutResources());
    			
    			if(dbg) System.out.println("MAXIMUN DEGREES ->  ingoing="+tg.getMaxIngoingDegree() + " - outgoing=" + tg.getMaxOutogingDegree());
    			

    			//cleaning
    			System.out.println("\n>>>>>>>>>>>CLEANING<<<<<<<<<<<<<<");

    			rdft = null;
    			rdfslabels = null;
    			rp = null;
    			tp = null;
    			System.gc();

    			System.out.println("\n>>>>>>>>>END CLEANING<<<<<<<<<<<<");
    			
    			TripleGraphBatch tgbatch = new TripleGraphBatch(graphName, tg.getNodeBatch(), tg.getPropBatch(),
    															tg.getResIngoingDegree(),tg.getResOutgoingDegree(), insert, basePath, tg.getMaxIngoingDegree(), tg.getMaxOutogingDegree());
    			
    			System.out.println("\n+++++++++++BATCH NODES=" + tg.getNodeBatch().size() + " AND PROPS=" + tg.getPropBatch().size() );
    			tgbatch.runBatchInsertions(basePath+"/graph.db");
    			
    			
    			if(wtf)
    			{
    				System.out.println("\n+++++++++++GENERATING DATA TO FILE:" +basePath);
    				
//    				DataWriter dw = new DataWriter(basePath, rdft, rdfslabels, 
//    						tg.getResIngoingDegree(), tg.getResOutgoingDegree(), 
//    						tgbatch.getNodeBatch(), tgbatch.getPropBatch());
//    				
    //
//    				dw.writeDataNodes();
//    				dw.writeDataRels();
    			}
    			
    			
    		} catch (NoSuchAlgorithmException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        	
    		System.out.println("\n+++++++++++DONE! :)" );
        	Date endDate = new Date();
        	String endTime = dateFormat.format(endDate);
    		System.out.println("END["+startTime+"->"+endTime+"]"); 
    	}
    	
    	
    	
    }
}
