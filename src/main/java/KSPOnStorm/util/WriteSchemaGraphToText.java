package KSPOnStorm.util;

import KSPOnStorm.Node;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;


import KSPOnStorm.Points;


public class WriteSchemaGraphToText {

	HashMap<Points, Double> grpahEdgeMap=new HashMap<Points, Double>();
	String fileAddress="C:\\Users\\esouser\\Desktop\\Graph\\Write\\stormSchemaGraph";

	public void writeSchemaGraph(HashMap<Points, Double> SchemaEdgeMap)
	{
		System.out.println("Writing starts:");
		 try {
		        File file = new File(fileAddress);
		        if(file.exists()==false)
		        {
		        	file.createNewFile();
		        	System.out.println("new file is created");
		        }
		        if(file.exists()){
		            FileWriter fw = new FileWriter(file,false);
		            BufferedWriter bw = new BufferedWriter(fw);
		            for(Points point:SchemaEdgeMap.keySet())
		            {
		            	String line="a "+point.snd+" "+point.tnd+" "+SchemaEdgeMap.get(point)+System.getProperty("line.separator");
						System.out.println(line);
		            	bw.write(line);
		            }
		            System.out.println("Writing is finished!");
		            bw.close(); 
		            fw.close();
		          
		        }
		    } catch (Exception e) {
		        // TODO: handle exception
		    }
	}
	public void writeASubGraph(HashMap<Node,List<Node>> subgraph)
	{

	}
}
