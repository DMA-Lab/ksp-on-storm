package KSPOnStorm.util;

import KSPOnStorm.Path;
import KSPOnStorm.Points;
import KSPOnStorm.Node;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;

public class WriteGraphToTxt {
    HashMap<Points, Double> grpahEdgeMap=new HashMap<Points, Double>();
    String fileAddress="C:\\Users\\esouser\\Desktop\\graph\\Write\\TextSubGraph";
    String fileEdgeAddress="C:\\Users\\esouser\\Desktop\\graph\\Write\\TextSubGraphEdgeMap";

    public void writeGraph(HashMap<Node,List<Node>> graph,HashMap<Points,Double>edgeMap,int subgraphID)
    {
        try {
            File file = new File(fileAddress);
            File fileEdge=new File(fileEdgeAddress);
            if(file.exists()==false)
            {
                file.createNewFile();
                System.out.println("new file is created");
            }
            if(file.exists()){
                FileWriter fw = new FileWriter(file,false);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write("The subgraph ID is "+subgraphID+System.getProperty("line.separator"));
                for(Node node:graph.keySet())
                {
                    for(int j=0;j<graph.get(node).size();j++)
                    {
                        String line=node.id+" "+graph.get(node).get(j).id+System.getProperty("line.separator");
                        bw.write(line);
                    }
                    //System.out.println(line);
                }
                System.out.println("Writing is finished!");
                bw.close();
                fw.close();
            }
            if(fileEdge.exists()==false)
            {
                fileEdge.createNewFile();
                System.out.println("new file is created");
            }
            if(fileEdge.exists()){
                FileWriter fw = new FileWriter(fileEdge,false);
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write("The subgraph ID is "+subgraphID+System.getProperty("line.separator"));
                for(Points point:edgeMap.keySet())
                {

                    String line=point.snd+" "+point.tnd+" "+edgeMap.get(point)+System.getProperty("line.separator");
                    bw.write(line);
                    //System.out.println(line);
                }
                System.out.println("Writing is finished!");
                bw.close();
                fw.close();
            }
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
    public void writePathList(List<Path> pathList, String fileAddress)
    {
        try {
            File file = new File(fileAddress);
            if(file.exists()==false)
            {
                file.createNewFile();
                System.out.println("new file is created");
            }
            if(file.exists()){
                FileWriter fw = new FileWriter(file,true);
                BufferedWriter bw = new BufferedWriter(fw);
                //bw.write("The subgraph ID is "+subgraphID+System.getProperty("line.separator"));
                for(Path path:pathList)
                {
                    String line="";
                    for(int j=0;j<path.line.size();j++)
                    {
                        line=line+path.line.get(j).id+"-";
                    }
                    line=line+", "+path.length;
                    line+=System.getProperty("line.separator");
                    bw.write(line);
                    //System.out.println(line);
                }
                System.out.println("Writing is finished!");
                bw.close();
                fw.close();
            }
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}

