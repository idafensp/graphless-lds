package oeg.upm.isantana.ldviz.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ResourceProperties {
	
	private Map<String, Map<String, Integer>>  resourceOutgoingProps;
	private Map<String, Map<String, Integer>>  resourceIngoingProps;
	
	private Map<String, Map<String, Double>>  normalizedResourceOutgoingProps;
	private Map<String, Map<String, Double>>  normalizedResourceIngoingProps;
	
	private NotRelevantProperties nrp;

	
	public ResourceProperties(NotRelevantProperties notrp)
	{
		this.resourceOutgoingProps = new HashMap<String, Map<String, Integer>>();
		this.resourceIngoingProps = new HashMap<String, Map<String, Integer>>();
		this.normalizedResourceOutgoingProps = new HashMap<String, Map<String, Double>>();
		this.normalizedResourceIngoingProps = new HashMap<String, Map<String, Double>>();
		
		this.nrp = notrp;
	}

	//SUMMING
	
	public void sumOutgoingProp(HashMap<String, HashSet<String>> resourceTypes,
			Map<String, Map<String, Integer>>  typePropCount)
	{
		sumProps(this.resourceOutgoingProps,resourceTypes,typePropCount);
	}
	
	public void sumIngoingProp(HashMap<String, HashSet<String>> resourceTypes,
			Map<String, Map<String, Integer>>  typePropCount)
	{
		sumProps(this.resourceIngoingProps,resourceTypes,typePropCount);
	}
	
	
	//for all resources of a type, return a list of 
	//<resources, property, count>
	private void sumProps(Map<String, Map<String, Integer>> inout, 
			HashMap<String, HashSet<String>> resourceTypes,
			Map<String, Map<String, Integer>>  typePropCount)
	{
		
    	Iterator it = resourceTypes.entrySet().iterator();
    	while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          
          
          String resource = (String) pair.getKey();

          System.out.println("***Analyzing:"+resource);
          
          for(String type : (HashSet<String>) pair.getValue())
          {
        	 Map<String, Integer> propCount = typePropCount.get(type);
          	 
        	 if(propCount == null)
        	 {
                 System.out.println("***>type:"+type+" has no count");

        		 continue; //this type has no count
        	 }

             System.out.println("***type:"+type+" has count= ");
        	 
        	 Iterator ipt = propCount.entrySet().iterator();
          	 while (ipt.hasNext()) {
                Map.Entry propCountPair = (Map.Entry)ipt.next();
               
                String prop = (String) propCountPair.getKey();
                Integer count = (Integer) propCountPair.getValue();
                
                putPropCount(inout, resource, prop, count);
                
          	 }
          }
    	}
	}

	
	private void putPropCount(Map<String, Map<String, Integer>> map, String res, String prop, Integer count)
	{
		System.out.println("****Adding to res=" + res + "-prop="+prop+"->"+count);
		if(!map.containsKey(res))
		{
			map.put(res, new HashMap<String, Integer>());
		}
		
		if(!map.get(res).containsKey(prop))
		{
			map.get(res).put(prop, count);
		}
		else
		{
			Integer current = map.get(res).get(prop);
			map.get(res).put(prop, current+count);
		}
	}

	//NORMALIZING
	
	public void normalizeOutgoingProps()
	{
		normalizePropertyCount(this.resourceOutgoingProps, this.normalizedResourceOutgoingProps);
	}
	
	public void normalizeIngoingProps()
	{
		normalizePropertyCount(this.resourceIngoingProps, this.normalizedResourceIngoingProps);
	}
	
	private void normalizePropertyCount(Map<String, Map<String, Integer>> propsCount, Map<String, Map<String, Double>> normalizedCount)
	{
		//find max and divide by it
		
		Iterator oit = propsCount.entrySet().iterator();
        while (oit.hasNext()) {
            Map.Entry pair = (Map.Entry)oit.next();
            String resource = (String) pair.getKey();
            Map<String, Integer> pc = (Map<String, Integer>) pair.getValue();

    		Integer max = -1;
        	Iterator oit2 = pc.entrySet().iterator();
            while (oit2.hasNext()) {
                Map.Entry pair2 = (Map.Entry)oit2.next();
                String prop = (String) pair2.getKey();
                Integer count = (Integer) pair2.getValue();
                
                if(count > max)
                	max = count;
            }
            
            normalizedCount.put(resource, new HashMap<String, Double>());

            oit2 = pc.entrySet().iterator();
            while (oit2.hasNext()) {
                Map.Entry pair2 = (Map.Entry)oit2.next();
                String prop = (String) pair2.getKey();
                Integer count = (Integer) pair2.getValue();
                
                Double norm = ((double) count) / ((double) max);
                normalizedCount.get(resource).put(prop, norm);                
            }
        }
        
		
		
	}
	
	
	//PRINTING
	private void printPropertiesCount(Map<String, Map<String, Integer>> countprops)
	{

    	Iterator oit = countprops.entrySet().iterator();
        while (oit.hasNext()) {
            Map.Entry pair = (Map.Entry)oit.next();
            String type = (String) pair.getKey();
            Map<String, Integer> pc = (Map<String, Integer>) pair.getValue();
            System.out.println(type+":");

        	Iterator oit2 = pc.entrySet().iterator();
            while (oit2.hasNext()) {
                Map.Entry pair2 = (Map.Entry)oit2.next();
                String prop = (String) pair2.getKey();
                Integer count = (Integer) pair2.getValue();
                System.out.println("--"+prop+"="+count);
            }    
            
        }
	}
	private void printPropertiesCountNormalized(Map<String, Map<String, Double>> countprops)
	{

    	Iterator oit = countprops.entrySet().iterator();
        while (oit.hasNext()) {
            Map.Entry pair = (Map.Entry)oit.next();
            String type = (String) pair.getKey();
            Map<String, Integer> pc = (Map<String, Integer>) pair.getValue();
            System.out.println(type+":");

        	Iterator oit2 = pc.entrySet().iterator();
            while (oit2.hasNext()) {
                Map.Entry pair2 = (Map.Entry)oit2.next();
                String prop = (String) pair2.getKey();
                Double count = (Double) pair2.getValue();
                System.out.println("--"+prop+"="+count);
            }    
            
        }
	}


	
	public void printOutgoingProperties()
	{
		this.printPropertiesCount(this.resourceOutgoingProps);
	}
	
	public void printIngoingProperties()
	{
		this.printPropertiesCount(this.resourceIngoingProps);
	}

	
	public void printNormalizedOutgoingProperties()
	{
		this.printPropertiesCountNormalized(this.normalizedResourceOutgoingProps);
	}
	
	public void printNormalizedIngoingProperties()
	{
		this.printPropertiesCountNormalized(this.normalizedResourceIngoingProps);
	}


	public Map<String, Map<String, Integer>> getResourceOutgoingProps() {
		return resourceOutgoingProps;
	}


	public Map<String, Map<String, Integer>> getResourceIngoingProps() {
		return resourceIngoingProps;
	}


	public Map<String, Map<String, Double>> getNormalizedResourceOutgoingProps() {
		return normalizedResourceOutgoingProps;
	}


	public Map<String, Map<String, Double>> getNormalizedResourceIngoingProps() {
		return normalizedResourceIngoingProps;
	}


	public void setResourceOutgoingProps(Map<String, Map<String, Integer>> resourceOutgoingProps) {
		this.resourceOutgoingProps = resourceOutgoingProps;
	}


	public void setResourceIngoingProps(Map<String, Map<String, Integer>> resourceIngoingProps) {
		this.resourceIngoingProps = resourceIngoingProps;
	}


	public void setNormalizedResourceOutgoingProps(Map<String, Map<String, Double>> normalizedResourceOutgoingProps) {
		this.normalizedResourceOutgoingProps = normalizedResourceOutgoingProps;
	}


	public void setNormalizedResourceIngoingProps(Map<String, Map<String, Double>> normalizedResourceIngoingProps) {
		this.normalizedResourceIngoingProps = normalizedResourceIngoingProps;
	}
	
	public double getNomalizedWeight(String res, String prop, Map<String, Map<String, Double>> normmap, String tag)
	{
		if(!normmap.containsKey(res))
		{
			//System.err.println("Resource="+res+" not found in normalized "+tag);
			return -1;
		}
		if(!normmap.get(res).containsKey(prop))
		{
			//System.err.println("Prop:"+prop+" for resource="+res+" not found in normalized "+tag);
			return -1;
		}
		return normmap.get(res).get(prop);
	}
	
	public double getNomalizedOutgoingWeight(String res, String prop)
	{
		return this.getNomalizedWeight(res, prop, normalizedResourceOutgoingProps, "outgoing");
	}
	
	public double getNomalizedIngoingWeight(String res, String prop)
	{
		return this.getNomalizedWeight(res, prop, normalizedResourceIngoingProps, "ingoing");
	}
	
}
