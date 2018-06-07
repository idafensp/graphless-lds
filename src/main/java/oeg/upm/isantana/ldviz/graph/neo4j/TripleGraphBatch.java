package oeg.upm.isantana.ldviz.graph.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import oeg.upm.isantana.ldviz.utils.FileStorage;


//http://neo4j.com/docs/stable/batchinsert-examples.html#_initial_import
public class TripleGraphBatch {
	
	private String graphName;
	private ArrayList<BatchNode> nodeBatch;
	private ArrayList<BatchRel> propBatch;

	private Map<String, Integer> resIngoingDegree;
	private Map<String, Integer> resOutgoingDegree;
	
	private int maxIngoingDegree;
	private int maxOutgoingDegree;

	private Map<String, Long> nodeGenIds;
	private boolean insert = false;
	private String outPath;
	
	
	public TripleGraphBatch(String gn, ArrayList<BatchNode> nodeBatch, ArrayList<BatchRel> propBatch,
						Map<String, Integer> indeg, Map<String, Integer> outdeg, boolean ins, String op, int mid, int mod) {
		super();
		this.graphName = gn;
		this.nodeBatch = nodeBatch;
		this.propBatch = propBatch;
		this.nodeGenIds = new HashMap<String, Long>();
		this.resIngoingDegree = indeg;
		this.resOutgoingDegree = outdeg;
		this.insert = ins;
		this.outPath = op;

		this.maxIngoingDegree = mid;
		this.maxOutgoingDegree = mod;
	}
	

	public void runBatchInsertions(String path) throws IOException
	{
		
		BatchInserter inserter = null;
		
		

	    RelationshipType graphRel = DynamicRelationshipType.withName(graphName);
		
		try
		{
		    inserter = BatchInserters.inserter( new File(path) );


		    Label graphLabel = DynamicLabel.label(graphName);
		    Label nodeLabel = DynamicLabel.label("Node");
		    Label literalLabel = DynamicLabel.label("Literal");
			if(insert )
			{
				try {
					inserter.createDeferredSchemaIndex(nodeLabel).on("nodeid").create();
				} catch (org.neo4j.graphdb.ConstraintViolationException e) {
					e.printStackTrace();
					insert=false;
				}
			}

			System.out.println("Inserting batch  " + this.nodeBatch.size() + " nodes, insert=" + insert);
	    	int step = this.nodeBatch.size() / 100 +1;
	    	int count = 0;
	    	int perc = 0;

			for(BatchNode bn : this.nodeBatch)
			{
				Label lab = nodeLabel;
				if(bn.isLiteral())
					lab = literalLabel;

				int ideg = getIngoingDeg(bn.getId());
				int odeg = getOutgoingDeg(bn.getId());
				
				double norIdeg = getNormalizedIngoingDeg(bn.getId());
				double norOdeg = getNormalizedOutgoingDeg(bn.getId());
				
				bn.addPropertyInteger("indeg",ideg);
				bn.addPropertyInteger("outdeg",odeg);
				bn.addPropertyInteger("deg",ideg+odeg);

				bn.addPropertyDouble("normindeg", norIdeg);
				bn.addPropertyDouble("normoutdeg", norOdeg);
				
				if(insert )
				{
					long genodeid = inserter.createNode(bn.getProperties(),graphLabel, lab);
					nodeGenIds.put(bn.getId(), genodeid);

			         if(count++%step == 0)
			        	  System.out.print(perc++ + "%...");

				}
				

			}
			
			if(insert)
			{
				

				System.out.println("Inserting batch  " + this.propBatch.size() + " properties, insert=" + insert);
		    	step = this.propBatch.size() / 100 +1;
		    	count = 0;
		    	perc = 0;
		    	int nullPropCount = 0;
		    	
		    	HashSet<String> nullPropList = new HashSet<String>();
		    	
				for(BatchRel bp : this.propBatch)
				{
					if((bp.getFromid() == null) || (bp.getToid() == null))
					{
						System.err.println(nullPropCount++ +")ERROR null from or to id:" + bp.toString());
						nullPropList.add(bp.toString());
						continue;
					}
				    long nfrom =-1;
				    long nto=-1;
					try {
						nfrom = nodeGenIds.get(bp.getFromid());
						nto = nodeGenIds.get(bp.getToid());
					} catch (NullPointerException e) {
						System.err.println(nullPropCount++ +")ERROR null from or to id:" + bp.toString());
						nullPropList.add(bp.toString());
						continue;
					}
				    
				    if(insert)
				    {
				    	inserter.createRelationship( nfrom, nto, graphRel, bp.getProperties());
				    }

			         if(count++%step == 0)
			        	  System.out.print(perc++ + "%...");
				}
				System.out.println();
				System.out.println("null found properties");			
				StringBuilder npstring = new StringBuilder();
				for(String np : nullPropList)
				{
					npstring.append(np);
					npstring.append("\n");
				}
				FileStorage.toFile(this.outPath+"_null_properties.txt", npstring.toString());
			}
		}
		finally
		{
		    if ( inserter != null )
		    {
		        inserter.shutdown();
		    }
		}
	}
	
	public void runBatchInsertionsTest(String lab, String path) throws IOException
	{
		BatchInserter inserter = null;
		
		try
		{
		    inserter = BatchInserters.inserter( new File(path) );

		    Label personLabel = DynamicLabel.label(lab);
		    inserter.createDeferredSchemaIndex( personLabel ).on("name").create();

		    Map<String, Object> properties = new HashMap<String,Object>();

		    properties.put( "name", "Mattias" );
		    long mattiasNode = inserter.createNode( properties, personLabel );

		    properties.put( "name", "Chris" );
		    long chrisNode = inserter.createNode( properties, personLabel );

		    RelationshipType knows = DynamicRelationshipType.withName( "KNOWS" );
		    inserter.createRelationship( mattiasNode, chrisNode, knows, null );
		}
		finally
		{
		    if ( inserter != null )
		    {
		        inserter.shutdown();
		    }
		    
		    System.out.println("Done");
		}
		
	}
	
	private int getIngoingDeg(String uri)
	{
		if(this.resIngoingDegree.containsKey(uri))
			return this.resIngoingDegree.get(uri);
		else
			return 0;
	}
	
	private double getNormalizedIngoingDeg(String uri)
	{
		if(this.resIngoingDegree.containsKey(uri))
			return ((double)this.resIngoingDegree.get(uri)) / ((double) this.maxIngoingDegree);
		else
			return 0.0;
	}
	
	private int getOutgoingDeg(String uri)
	{
		if(this.resOutgoingDegree.containsKey(uri))
			return this.resOutgoingDegree.get(uri);
		else
			return 0;
	}
	
	private double getNormalizedOutgoingDeg(String uri)
	{
		if(this.resOutgoingDegree.containsKey(uri))
			return ((double)this.resOutgoingDegree.get(uri)) / ((double) this.maxOutgoingDegree);
		else
			return 0;
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
}
