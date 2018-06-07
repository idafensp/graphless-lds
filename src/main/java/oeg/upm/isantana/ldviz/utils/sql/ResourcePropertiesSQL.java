package oeg.upm.isantana.ldviz.utils.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import oeg.upm.isantana.ldviz.sparql.sql.SQLConstants;
import oeg.upm.isantana.ldviz.sparql.sql.SQLHelper;
import oeg.upm.isantana.ldviz.utils.NotRelevantProperties;

public class ResourcePropertiesSQL {
	
//	private Map<String, Map<String, Integer>>  resourceOutgoingProps;
//	private Map<String, Map<String, Integer>>  resourceIngoingProps;
//	
//	private Map<String, Map<String, Double>>  normalizedResourceOutgoingProps;
//	private Map<String, Map<String, Double>>  normalizedResourceIngoingProps;
	
	private NotRelevantProperties nrp;
	private SQLHelper sqlh;

	
	public ResourcePropertiesSQL(NotRelevantProperties notrp, String graphName)
	{
		this.nrp = notrp;
		sqlh = new SQLHelper(graphName);
	}

	//SUMMING

	public void sumOutgoingProp()
	{
		this.sumProps(SQLConstants.NORM_RP_COUNT_OUT, SQLConstants.RTP_COUNT_OUT);	
	}
	

	public void sumIngoingProp()
	{
		this.sumProps(SQLConstants.NORM_RP_COUNT_ING, SQLConstants.RTP_COUNT_ING);	
	}
	
	public void sumProps(String normTableName, String countTableName)
	{
		
		System.out.println(">>>sumProps>>"+ normTableName +" & "+countTableName);

		sqlh.createTableRPCountNorm(normTableName);
		ResultSet res = sqlh.selectRTPCountsSum(countTableName);
		
		try {
			
			while(res.next())
			{
				String resource = res.getString("resource");
				String property = res.getString("property");
				int count = res.getInt(3);
				
				//we insert count as the max, later we will see if we need to divide
				sqlh.insertRPCountNorm(normTableName, resource, property, count, (double) count);
			}
						
			
			ResultSet resMax = sqlh.selectRPMaxCount(normTableName);
			while(resMax.next())
			{
				String resource = resMax.getString("resource");
				int max = resMax.getInt(2);
				
				//no need to divide by one
				//as it is order by, once we reach 1, we can stop
				if(max==1) 
				{
					System.out.println(":......:-MAX 1-:......");
					System.out.println(": " + resource);
					System.out.println(":......:-BREAK-:......");
					break;
				}
				
				System.out.println(">>>RES MAX="+ max +"-"+resource);
				
				sqlh.updateMaxNorm(normTableName, resource, max);

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if(res!=null)
					res.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
