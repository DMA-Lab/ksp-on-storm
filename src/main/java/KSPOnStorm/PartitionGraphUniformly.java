package KSPOnStorm;

import java.util.*;

public class PartitionGraphUniformly {

	int hopCount=800;
	List<Partition> partitionList=null;
    HashSet<Integer> visitedNodeIdSet=null;
    List<Node> seedNodeQueue=null;
    public List<Partition> divide(HashMap<Node, List<Node>> graphC)//, HashMap<Points,Integer> edgeSetC
    {
    	 partitionList=new ArrayList<Partition>();
    	 HashMap<Node, List<Node>> graph=graphC;
    //	 PrintFunctions.printGraph(graph);
    	 HashMap<Node, Integer> adjNodeSize=getAdjNodeSize(graph); //keep the adjacent degree of every node
    	// printGraph(graph);
    	 seedNodeQueue=new ArrayList<Node>();
    	 List<Node> nodeList=new ArrayList<Node>();
    	 nodeList.addAll(graph.keySet());
    	 seedNodeQueue.add(nodeList.get(0));
    	 while(!seedNodeQueue.isEmpty())
  		{
    		 Node seedNode=seedNodeQueue.remove(0);
    		 if (graph.get(seedNode).size()==0)
    		 {
    			 continue;
    		 }
    		 Partition par=new Partition();
    		 par.subGraph.put(seedNode, new LinkedList<Node>());
    		 par.boundaryNodeList.add(seedNode);
    		 partitionList.add(par);
			int steps=0;
    		 Vector<Node> adjNodeVector=new Vector<Node>();
    		 adjNodeVector.add(seedNode);
    		 ArrayList<Node> visitedNodeList=new ArrayList<Node>();
    		 //visitedNodeList.clear();
    		 while(!adjNodeVector.isEmpty())
    		 {
    			 Node startNode=adjNodeVector.remove(0);
    			 steps++;    //records the scale of subgraph
    			 if(!visitedNodeList.contains(startNode))
    			 {
    				 visitedNodeList.add(startNode);
    			 }
    			 List<Node> adjNodeSet=new LinkedList<Node>();
  		   	     adjNodeSet.addAll(graph.get(startNode));
    			 for(Node node:adjNodeSet)
    			 {
    				 if(!visitedNodeList.contains(node))
    				 {
	    				 if(adjNodeSize.get(node)>graph.get(node).size())
	    				 {
	    					 if (!par.boundaryNodeList.contains(node))
	    					 {
	    						 par.boundaryNodeList.add(node);
	    					 }
	    				 }
	    				 visitedNodeList.add(node);
    				 }
    				 par.subGraph.get(startNode).add(node);
    				 if(!par.subGraph.containsKey(node))
    				 {
    					 par.subGraph.put(node, new LinkedList<Node>());
    				 }
    				 par.subGraph.get(node).add(startNode);
    				 graph.get(startNode).remove(node);
		    	     graph.get(node).remove(startNode);
		    	     if(graph.get(node).size()!=0)
		    	     {
		    	    	 adjNodeVector.add(node);
		    	     }
    			 }
    			 if((steps<this.hopCount)==false)
    			 {
    				 for(int m=0;m<adjNodeVector.size();m++)
    	    		 {
    	    			 if (graph.get(adjNodeVector.get(m)).size()!=0)
    	    			 {
    	    				 seedNodeQueue.add(adjNodeVector.get(m));
    	    				 if (!par.boundaryNodeList.contains(adjNodeVector.get(m)))
    	    				 {
    	    					 par.boundaryNodeList.add(adjNodeVector.get(m));
    	    				 }
    	    			 }
    	    		 }
    	    	//	 seedNodeQueue.addAll(adjNodeVector);
    	    		 adjNodeVector.clear();
    			 }
    		 }
  		}
    	 //System.out.println("Divide is finished");
		return partitionList;
    }
    public HashMap<Node, Integer> getAdjNodeSize(HashMap<Node, List<Node>> graph)
    {
   	 HashMap<Node, Integer> adjNodeSize=new HashMap<Node, Integer>();
   	 Iterator<Node> itera=graph.keySet().iterator();
   	 while(itera.hasNext())
   	 {
   		 Node node=(Node)itera.next();
   		 adjNodeSize.put(node, graph.get(node).size());
   	 }
   	 return adjNodeSize;
    }
    public static void printBoundaryNodes(Vector<Node> boundaryNode)
    {
    	for(Node node:boundaryNode)
    	{
    		System.out.println("Boundary Node Id: "+node.id);
    	}
    }
}
