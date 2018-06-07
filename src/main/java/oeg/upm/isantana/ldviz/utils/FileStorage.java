package oeg.upm.isantana.ldviz.utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class FileStorage {

	
	public static void toFile(String path, String content)
	{
		try {
			PrintWriter out = new PrintWriter(path);
			out.println(content);
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
