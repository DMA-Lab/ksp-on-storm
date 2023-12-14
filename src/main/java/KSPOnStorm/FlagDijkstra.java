package KSPOnStorm;

import java.util.*;

public class FlagDijkstra {
	
	//HashMap<Points,Integer> edgeSet;
		HashMap<Points, Path> shortestPathMap=new HashMap<Points, Path>();
		HashMap<Points,List<Node>> simpleSPathMap;

		public List<Node> dijkstraForTwoNodes(String boltID,int startNodeId, int endNodeId, Map<Node, List<Node>> graph, Map<Points,Double> edgeSet, Map<Points,Map<String,Boolean>> yenFlagMap)
		{
			/*This algorithm will return the shortest path from the startNode to the endNode */
			for(Node node:graph.keySet())
			{
				node.dijkstraDistance=Double.MAX_VALUE;
			//	System.out.println(boltID+" dijkstraDistance of node "+node.id+": "+node.dijkstraDistance);
			}

			Node startNode=null;
			for(Node node:graph.keySet())
			{
				if(node.id==startNodeId)
				{
					startNode=node;
					break;
				}
			}
			simpleSPathMap=new HashMap<Points,List<Node>>();
			Queue<Node> visitedNodeQueue=new PriorityQueue<Node>();

			if(startNode==null)
			{
				System.out.println("StartNode is null！");
			}
			startNode.dijkstraDistance=0.0;
			//nodeDistanceMap.put(startNode.id, 0.0);

			List<Node> intialPath=new LinkedList<Node>();
			intialPath.add(startNode);
			simpleSPathMap.put(new Points(startNode.id,startNode.id),intialPath);
		//	reachedNodeStack.add(startNode);
			visitedNodeQueue.offer(startNode);
			while(!visitedNodeQueue.isEmpty())
			{
				Node nextNode=visitedNodeQueue.poll();
				//System.out.println("Dij: "+boltID+" DijDistance  of NextNode "+nextNode.id +" is "+nextNode.dijkstraDistance);
				nextNode.setDij(boltID, true);
				//System.out.println("Dij: boltID  "+boltID+"， NodeID "+nextNode.id+"its dij flag of "+nextNode.getDij(boltID));
				if(nextNode.id==endNodeId)
				{
					Points point=new Points(startNode.id, nextNode.id);
					initializeGraphFlag(boltID,graph);
					return simpleSPathMap.get(point);
				}

			//	unVisitedNodeList.remove(nextNode);

				Points snPoint=new Points(startNode.id,nextNode.id);
			//	System.out.println("Dij: nextNodeId: "+nextNode.id);
				for(int i=0;i<graph.get(nextNode).size();i++)
				{
					Node childNode=graph.get(nextNode).get(i);

					if(childNode.getDij(boltID)==false)
					{
						Points ncPoint=new Points(nextNode.id,childNode.id);
						Points scPoint=new Points(startNode.id,childNode.id);
						double ncPointValue=0.0;

						if(yenFlagMap.get(ncPoint).get(boltID)==true)
                        {
                            ncPointValue=Double.MAX_VALUE;
						//	System.out.println("Dij: "+boltID);
                        }
                        else
                        {
                             ncPointValue=edgeSet.get(ncPoint);
                        }
                        //double ncPointValue=edgeSet.get(ncPoint);
				//		System.out.println("Dij: "+boltID+" DijDistance  of NextNode "+nextNode.id +" is "+nextNode.dijkstraDistance);
				//		System.out.println("Dij: "+boltID+" distance (ncPointValue) between nodes "+ncPoint.snd +", "+ncPoint.tnd+" is "+ncPointValue);
						double updatedValue=nextNode.dijkstraDistance+ncPointValue;
						//System.out.println(boltID+" "+childNode.toString());
						//System.out.println("Dij: "+boltID+" DijDistance of ChildNode "+childNode.id +" is "+childNode.dijkstraDistance);
						if(updatedValue<childNode.dijkstraDistance)
						{
							//nodeDistanceMap.put(childNode.id,updatedValue);
							List<Node> extendPathList=new LinkedList<Node>();
						//	simpleSPathMap.get(snPoint).add(childNode);
							extendPathList.addAll(simpleSPathMap.get(snPoint));
							extendPathList.add(childNode);
							simpleSPathMap.put(scPoint,extendPathList);

							childNode.dijkstraDistance=updatedValue;
						//	System.out.println("Dij: "+boltID+" new dijDistance of ChildNode:" +childNode.dijkstraDistance);
							visitedNodeQueue.offer(childNode);
						//	System.out.println("Dij: "+boltID+" add childNode "+childNode.id+" into queue!");
						//	System.out.println(childNode.id);
						}
					}
					else
					{
					//	System.out.println("Dij: "+boltID+" childNode  "+childNode.id+" dijFlag is true ");
					}
				}

			}
			initializeGraphFlag(boltID,graph);
			return null;
		}
	public void initializeGraphFlag(String boltID,Map<Node, List<Node>> graph)
	{
		for(Node node:graph.keySet())
		{
			node.setDij(boltID,false);
			for(Node adjNode:graph.get(node))
			{
				adjNode.setDij(boltID,false);
			}
		}
	}
}
