package KSPOnStorm;

import java.util.*;

public class PrintFunctions {
	
	public static void printGraph(HashMap<Node, List<Node>> graph)
    {
   	 Iterator itera=graph.keySet().iterator();
   	 while(itera.hasNext())
   	 {
   		 Node node=(Node)itera.next();
   		 List<Node> adjNode=graph.get(node);
   		 System.out.println("NodeID: "+node.id);
   		 for(Node no:adjNode)
   		 {
   			 System.out.println("adjNodeId: "+no.id);
   		 }
   		 System.out.println("--------------------");
   	 }
   	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>");
   	 System.out.println("One subgraph is finished!");
    }
	
	 public static void printBoundaryNodes(Vector<Node> boundaryNode)
	    {
	    	for(Node node:boundaryNode)
	    	{
	    		System.out.println("Boundary Node Id: "+node.id);
	    	}
	    }
	 
	public static void printPath(List<Node> lbpath, String pathName)
	{
		System.out.println(pathName+" is:");
		for(Node node:lbpath)
		{
			System.out.print(node.id+"-");
		}
		System.out.println();
	}
	public static void printKSP(List<List<Node>> kShortestPathVector)
	{
		for(int i=0;i<kShortestPathVector.size();i++)
		{
			System.out.println("The "+i+"th path is ");
			for(int j=0;j<kShortestPathVector.get(i).size();j++)
			{
				System.out.print(kShortestPathVector.get(i).get(j).id+"-");
			}
			System.out.println();
			System.out.println("Length: "+kShortestPathVector.get(i).size());
		}
	}
	public static void printSubGraphLowerBoundPaths(List<SubGraph> subgraphList)
	{
		for(SubGraph sg:subgraphList)
		{
			System.out.println("SubgraphId: "+sg.subgraphID);
			for(Points point:sg.lowerBoundPath.keySet())
			{
				System.out.println("FirstBoundNodeID "+point.snd+"SecondBoundNodeID "+point.tnd+":");
				for(List<Node> lbpath:sg.lowerBoundPath.get(point))
				{
					PrintFunctions.printPath(lbpath, "lbpath");
				}
			}
		}
	}
	public static void printSchemaGraph(HashMap<Node,HashSet<Node>> schemaGraph)
	{
		for(Node node:schemaGraph.keySet())
		{
			System.out.println("Node: "+node.id+", its adjacent nodes:");
			for(Node adjNode:schemaGraph.get(node))
			{
				System.out.print(adjNode.id+", ");
			}
			System.out.println();
		}
	}
	public static void printEdgeSet(HashMap<Points, Double> EdgeMap)
	{
		System.out.println("The edge set is: ");
		for(Points point:EdgeMap.keySet())
		{
			System.out.println("node1: "+point.snd+", node2: "+point.tnd+", length: "+EdgeMap.get(point));
		}
		System.out.println("---------------------------------------");
	}
	public static void printSchemaEdge(HashMap<Points, Double> minimumEdgeMap)
	{
		System.out.println("The schema graph paths between boundary nodes:");
		for(Points point:minimumEdgeMap.keySet())
		{
			System.out.println("boundaryNodeID1: "+point.snd+", boundaryNodeID2:"+point.tnd+", hypothetical path length is "+minimumEdgeMap.get(point));
		}
	}
	public static void printLowerBoundPaths(HashMap<Points,List<List<Node>>> lowerBoundPath)
	{
		for(Points point:lowerBoundPath.keySet())
		{
			System.out.println("node1: "+point.snd+", node2: "+point.tnd+", paths "+lowerBoundPath.get(point).size()+" : ");
			for(List<Node> path:lowerBoundPath.get(point))
			{
				if(path.size()==0)
				{
					System.out.println("The size is 0!!!");
					continue;
				}
				for(Node node:path)
				{
					System.out.print(node.id+", ");
				}
				System.out.println();
			}
		}
	}
	public static void printFinalShortestPaths(List<Path> finalPathList)
	{
		for(Path fpath:finalPathList)
		{
			for(Node node:fpath.line)
			{
				System.out.print(node.id+", ");
			}
			System.out.println( );
		}
	}
}
