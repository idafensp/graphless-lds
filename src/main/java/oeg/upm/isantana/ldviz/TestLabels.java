package oeg.upm.isantana.ldviz;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;

import oeg.upm.isantana.ldviz.sparql.RDFSLabels;

public class TestLabels {

	private static RDFSLabels rdfslabels;

    public static void main( String[] args )
    {
    	rdfslabels = new RDFSLabels("sparqlEndpointUrl", "namedGraph", null,0, 0, 0);//dummy
    	
		Model model = ModelFactory.createDefaultModel();
		Resource r = model.createResource("http://dbtune.org/jamendo/track/23691");
		
		System.out.println("Label=" + getLabel(r));
    }	

    
	
	private static String getLabel(Resource res)
	{
		try {
			if (rdfslabels.getResourceLabels().containsKey(res.getURI()))
				return rdfslabels.getResourceLabels().get(res.getURI()).get(0); //TODO now we are  getting only the first 
			else {
				if ((res.getLocalName() != null) && (!res.getLocalName().isEmpty())) {

					System.out.println("is localname");
					return res.getLocalName();
				} else {
					String uri = res.getURI();
					if (uri.indexOf("/") > 0) {
						uri = uri.substring(uri.lastIndexOf("/")+1, uri.length());
						System.out.println("is /");

						if (uri.indexOf("#") > 0) {
							uri = uri.substring(uri.lastIndexOf("#")+1);
							System.out.println("is #");
						}	
						if(uri.length()>1)
						{	
							return uri;
						}
					}
					else 
					{
						if (uri.indexOf("#") > 0) {
							uri = uri.substring(uri.lastIndexOf("#")+1);
							System.out.println("is #");
							if(uri.length()>1)
								return uri;
						}
						else
						{
							System.out.println("is done");
							int len = res.getURI().length()-15;
							if(len < 0)
							{
								len = 0;
							}
							return "..."+res.getURI().toString().substring(len);
						}
					}
					
					return uri;
				}
			} 
		} catch (Exception e) {
			System.err.println("Error while getting label for " + res.toString());
			e.printStackTrace();
		}
		return "...";
	}
}
