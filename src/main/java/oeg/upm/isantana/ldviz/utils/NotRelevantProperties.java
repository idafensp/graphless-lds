package oeg.upm.isantana.ldviz.utils;

import java.util.ArrayList;
import java.util.List;

public class NotRelevantProperties {
	
	private List<String> notProps;
	
	//TODO read them froma file or something
	public NotRelevantProperties(String filePath)
	{
		notProps = new ArrayList<String>();
		notProps.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		notProps.add("http://www.w3.org/2000/01/rdf-schema#label");
		notProps.add("http://www.w3.org/2000/01/rdf-schema#comment");
	}
	
	public boolean irrelevant(String uri)
	{
		return notProps.contains(uri);
	}

}
