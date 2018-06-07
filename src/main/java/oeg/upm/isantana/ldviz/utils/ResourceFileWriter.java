package oeg.upm.isantana.ldviz.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class ResourceFileWriter {
	
	public static void listToFile(String path, List<String> list) throws IOException
	{
		System.out.println("Wrting file to :" + path);
		FileWriter writer = new FileWriter(path); 
		for(String str: list) {
			writer.write(str+"\n");
		}
		writer.close();
	}
	
	public static void listToFile(String path, HashSet<String> list) throws IOException
	{
		System.out.println("Wrting file to :" + path);
		FileWriter writer = new FileWriter(path); 
		for(String str: list) {
			writer.write(str+"\n");
		}
		writer.close();
	}

}
