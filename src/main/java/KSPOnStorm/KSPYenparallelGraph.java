package KSPOnStorm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KSPYenparallelGraph {
	// List<List<Node>> canidatePathList=new ArrayList<List<Node>>();
	public List<List<Node>> kspYenWithoutLoops(String boltID, int k, int startNodeId, int endNodeId, Map<Node, List<Node>> graph, Map<Points,Double> edgeSet, Map<Points,Map<String,Boolean>> yenFlagMap) throws InterruptedException //TimeThread tt)
	{
		/*This algorithm returns k shortest paths in which loops are allowed*/
		//HashMap<Node, List<Node>> firstCopyGraph=duplicateGraph(graph);
		//Map<Points,Map<String,Boolean>> yenFlagMap=new ConcurrentHashMap<>();
		//System.out.println("Yen: query first node id: "+startNodeId+", second node id:"+ endNodeId);

		createYenFlagMap(boltID,yenFlagMap,edgeSet);

		List<Integer> lengthList=new ArrayList<Integer>();//record the length of paths
		FlagDijkstra  dij=new FlagDijkstra();

		List<Node> shortestPath=dij.dijkstraForTwoNodes(boltID,startNodeId, endNodeId, graph, edgeSet,yenFlagMap);
		if(shortestPath==null)
		{
			System.out.println("Yen: boltID "+boltID+" There is no the first shortest path between "+startNodeId+", "+endNodeId);
		}
		List<List<Node>> canidatePathList=new ArrayList<List<Node>>();     //keep the path from every deviation node to the destination
		List<List<Node>> kShortestPathList=new ArrayList<List<Node>>();    //keep the k shortest paths between startNode and endNode
		Queue<List<Node>> baseLinePathQueue=new LinkedList<List<Node>>();  //a queue to keep the baseline path be processing currently


		baseLinePathQueue.offer(shortestPath);                                  //process the shortest path
		kShortestPathList.add(shortestPath);                                  //keep the shortest path
//		lengthList.add(shortestPath.size());
		//printPath(shortestPath,"shortestPath");
		//int loop=0;
		if(kShortestPathList.size()==k)
		{
			return kShortestPathList;
		}
		while(!baseLinePathQueue.isEmpty())
		{
			//	 long startTime = System.currentTimeMillis();
			//loop++;
			boolean newpathFlag=false;                                          //to label whether there is a generated new path
			List<Node> baseLinePath=baseLinePathQueue.poll();
			//	PrintFunctions.printPath(baseLinePath, "baseLinePath");
			ExecutorService executorService = Executors.newCachedThreadPool();
			final CountDownLatch cdl = new CountDownLatch(baseLinePath.size()-1);
			for(int i = 0; i<(baseLinePath.size()-1); i++)                         //process every deviation node on the baseline path
			{
				//HashMap<Node, List<Node>> copyGraph=duplicateGraph(graph);
				Vector<Node> routPath=new Vector<Node>();
				Node deviationSNode=baseLinePath.get(i);
				Node deviationENode=baseLinePath.get(i+1);
				Points point=new Points(deviationSNode.id,deviationENode.id);
				Points point1=new Points(deviationENode.id,deviationSNode.id);
				yenFlagMap.get(point).put(boltID,true);
				yenFlagMap.get(point1).put(boltID,true);
				// System.out.println("boltComponentID: "+boltID+ "set edge between node "+point.snd+" and node "+point.tnd+" as true");
				// System.out.println("boltComponentID: "+boltID+ "set edge between node "+point1.snd+" and node "+point1.tnd+" as true");

				//List<Node> deviationPath=new LinkedList<Node>();

				//deviationPath=dij.dijkstraForTwoNodes(boltID,deviationSNode.id, endNodeId, graph, edgeSet,yenFlagMap);
				executorService.execute( new FlagYen(boltID,startNodeId, endNodeId, graph,  edgeSet, yenFlagMap, cdl,routPath,baseLinePath, i,canidatePathList));
				//canidatePathList.add(new List<Node>( ));

//				if(deviationPath==null)
//				{
//					// System.out.println("startNode: "+deviationSNode.id+" endNode: "+endNode.id+ "have no deviation shortest path");
//				}
//				else
//				{
//					for(int h=0;h<i;h++)
//					{
//						routPath.add(baseLinePath.get(h));
//					}
//					Vector<Node> candidatePath=new Vector<Node>();
//					{
//						candidatePath.addAll(routPath);
//						candidatePath.addAll(deviationPath);
//					}
//					// long startTime1 = System.currentTimeMillis();
//					if(containPath(canidatePathList,candidatePath)==false)
//					{
//						canidatePathList.add(candidatePath);
//						newpathFlag=true;                                                       // this flag means a new path is generated
//					}
//					//	 tt.addLoopingTime(System.currentTimeMillis() - startTime1);
//				}
			}

			cdl.await();

			if(newpathFlag==false)
			{
				break;
			}
			if(canidatePathList.isEmpty())
			{
				break;
			}
			List<Node> potentialPath=searchShortestPath(canidatePathList);
			canidatePathList.remove(potentialPath);

			if(kShortestPathList.size()<k)
			{
				baseLinePathQueue.offer(potentialPath);
				kShortestPathList.add(potentialPath);
			}
		}
		return kShortestPathList;
	}

	public void  createYenFlagMap(String boltID,Map<Points,Map<String,Boolean>> yenFlagMap,Map<Points,Double> edgeSet)
	{
		//boltID=boltID;
		for(Points point:edgeSet.keySet())
		{
			Points newPoint=new Points(point.snd,point.tnd);
			Map<String,Boolean> flagMap=new ConcurrentHashMap<>();
			flagMap.put(boltID,false);
			yenFlagMap.put(newPoint,flagMap);
		}
	}
	public boolean judgePathEqual(List<Node> spath, List<Node> epath)
	{
		if(spath.size()!=epath.size())
		{
			return false;
		}
		else
		{
			for(int i=0;i<epath.size();i++)
			{
				if(spath.get(i).id!=epath.get(i).id)
				{
					return false;
				}
			}
		}
		return true;
	}
	public boolean containPath(List<List<Node>> canidatePathList, Vector<Node>path)
	{
		if(canidatePathList.size()==0)
		{
			return false;
		}
		for(List<Node> cPath:canidatePathList)
		{
			if(judgePathEqual(cPath,path)==true)
			{
				return true;
			}
		}
		return false;
	}

	public List<Node> searchShortestPath(List<List<Node>> canidatePathVector)
	{
		List<Node> spath=new Vector<Node>();
		int minLength=Integer.MAX_VALUE;
		for(List<Node> path:canidatePathVector)
		{
			if(path.size()<minLength)
			{
				minLength=path.size();
				spath=path;
			}
		}
		return spath;
	}
//	public HashMap<Node, List<Node>> duplicateGraph (HashMap<Node, List<Node>> graph)//, TimeThread tt)
//	{
//		//long start = System.currentTimeMillis();
//		HashMap<Node, List<Node>> copyGraph=new HashMap<Node, List<Node>>();
//		Iterator<Node> iterator=graph.keySet().iterator();
//
//		while (iterator.hasNext())
//		{
//			Node keyNode=(Node)iterator.next();
//			Vector<Node> nodeList=new Vector<Node> ();
//			for(int i=0;i<graph.get(keyNode).size();i++)
//			{
//				Node node=(Node)graph.get(keyNode).get(i).clone();
//				node.setDij(Thread.currentThread().getId(),false);
//				nodeList.add(node);
//			}
//			copyGraph.put(keyNode, nodeList);
//		}
//	//	tt.addLoopingTime(System.currentTimeMillis() - start);
//		return copyGraph;
//	}
}
