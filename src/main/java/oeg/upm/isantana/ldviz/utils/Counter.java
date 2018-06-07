package oeg.upm.isantana.ldviz.utils;

public class Counter {
	
	private static int counter = 0;
	
	public static void increase()
	{
		counter ++;
		System.out.println("~~~" + counter + " queries executed ~~~");
	}

}
