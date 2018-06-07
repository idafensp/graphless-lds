package oeg.upm.isantana.ldviz.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;

import oeg.upm.isantana.ldviz.exceptions.NoTopPropertyException;

public class FilterRDFType {

	private OntModel omod;
	private Model dmod;
	private  Map<String, Integer> depthMap;
	private Map<String, String> leafType;

	private Model indOntoTypes;
	private Model indOtherTypes;


	public FilterRDFType(String ontPath)
	{
		this.omod = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
		
		// Use the FileManager to find the input file
		InputStream in = FileManager.get().open(ontPath);
	
		if (in == null)
			throw new IllegalArgumentException("File: "+ontPath+" not found");
	
		// Read the RDF/XML file
		this.omod.read(in, null);
		
		this.depthMap = new HashMap<String, Integer>();
		this.indOntoTypes = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
		this.indOtherTypes = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);

	}
	

	public FilterRDFType(OntModel m)
	{
		this.omod = m;
		this.depthMap = new HashMap<String, Integer>();	
		this.indOntoTypes = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
		this.indOtherTypes = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);

	}
	
	
	//calculates the depth degree of each type on the onto
	//level 0 is the topClassUri (usually owl:Thing)
	public void generateOntoLevels(String topClassURI) throws NoTopPropertyException
	{
		OntClass topClass = omod.getOntClass(topClassURI);
		
		if(topClass == null)
		{
			throw new NoTopPropertyException();
		}
		
		
		ArrayList<OntClass> loc = new ArrayList<OntClass>();
		loc.add(topClass);
		
		System.out.println("Start calculating..." + loc.size() );
		calculateSubclasses(loc,0);

	}
	
	//recursive method to travese the ontology taxonomy tree in depth
	private void calculateSubclasses(ArrayList<OntClass> ol, int depth)
	{
		for(OntClass oc : ol)
		{
			System.out.println("Calculating for: " + oc.getURI() + "@" + depth);

			this.depthMap.put(oc.getURI(), depth);
			ExtendedIterator<OntClass> it = oc.listSubClasses();
			ArrayList<OntClass> loc = new ArrayList<OntClass>();
			
			while(it.hasNext())
			{
				loc.add(it.next());	
			}
			
			calculateSubclasses(loc,depth+1);
			
		}
	}
	

	//load the RDF data with the property type assertions
	public void loadDataRDFType(String path)
	{
		this.dmod = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
		
		// Use the FileManager to find the input file
		InputStream in = FileManager.get().open(path);
	
		if (in == null)
			throw new IllegalArgumentException("File: "+path+" not found");
	
		// Read the data file
		this.dmod.read(in, null, "Turtle");
		
	}
	
	
	//collects for each resource its list of types
	//depends on the depth calculation to be done first
	public void collectResourceRDFType()
	{
		StmtIterator it = this.dmod.listStatements();
		
		leafType = new HashMap<String, String>();
		
		while(it.hasNext())
		{
			Statement st = it.next();
			Resource sub = st.getSubject();
			Resource obj = (Resource) st.getObject();
			
			Integer d = depthMap.get(obj.getURI());
			
			if(d==null) //not a type of the onto taxonomy
			{	
				this.indOtherTypes.add(st); //add it to the others model
			}
			else //contained in the taxonomy
			{
				
				this.indOntoTypes.add(st); //add it to the onto model

				//add if max
				addMaxLeafType(d, sub.getURI(),obj.getURI());
								
			}
		}
	}
	
	


	public OntModel getOmod() {
		return omod;
	}


	public Model getDmod() {
		return dmod;
	}


	public Map<String, Integer> getDepthMap() {
		return depthMap;
	}


	public void setOmod(OntModel omod) {
		this.omod = omod;
	}


	public void setDmod(Model dmod) {
		this.dmod = dmod;
	}


	public void setDepthMap(Map<String, Integer> depthMap) {
		this.depthMap = depthMap;
	}


	public Map<String, String> getLeafType() {
		return leafType;
	}


	public void setLeafType(Map<String, String> leafType) {
		this.leafType = leafType;
	}


	public Model getIndOntoTypes() {
		return indOntoTypes;
	}


	public Model getIndOtherTypes() {
		return indOtherTypes;
	}


	public void setIndOntoTypes(Model indOntoTypes) {
		this.indOntoTypes = indOntoTypes;
	}


	public void setIndOtherTypes(Model indOtherTypes) {
		this.indOtherTypes = indOtherTypes;
	}
	
	public void addMaxLeafType(Integer d, String res, String type)
	{
		//check if this type is deeper
		//we already have a level
		if(this.leafType.containsKey(res))
		{
			//if the new depth is higher than the one we had for that type
			
			String currentType = this.leafType.get(res);
			if(d > depthMap.get(currentType))
			{
				this.leafType.put(res, type);
			}
		}
		else //first time, we add it
		{
			this.leafType.put(res, type);	
		}
	}
	
	public void writeIndOntoTypes(String path, String format)
	{
	    OutputStream out;
		try {
			out = new FileOutputStream(path);
			this.indOntoTypes.write(out, format);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeIndOtherTypes(String path, String format)
	{
	    OutputStream out;
		try {
			out = new FileOutputStream(path);
			this.indOtherTypes.write(out, format);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	
	

}
