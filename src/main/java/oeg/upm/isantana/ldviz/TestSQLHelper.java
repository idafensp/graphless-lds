package oeg.upm.isantana.ldviz;

import oeg.upm.isantana.ldviz.sparql.sql.SQLConstants;
import oeg.upm.isantana.ldviz.sparql.sql.SQLHelper;

public class TestSQLHelper {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SQLHelper sqlh = new SQLHelper("TGN");

		sqlh.init();
		
		
		sqlh.createTableRTPCount(SQLConstants.RTP_COUNT_ING);
		
		int i = 0;
		while(i++<10)
		{
			sqlh.insertRTPCount(SQLConstants.RTP_COUNT_ING, "http://es.dbpedia.org/page/Miguel_de_Cervantes_"+i, 
														"http://es.dbpedia.org/ontology/Person", 
														"http://es.dbpedia.org/ontology/genre", i);
		}

	}

}
