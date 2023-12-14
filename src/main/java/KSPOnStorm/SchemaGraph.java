package KSPOnStorm;

import KSPOnStorm.VirtualHopsBolt.VirtualHopsQueryProcessingBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaGraph implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(SchemaGraph.class);
	public HashMap<Node,List<Node>> schemaGraph;
	//public HashMap<Node,List<Node>> schemaGraphList;
	public HashMap<Points,PriorityQueue<Double>> schemaEdgeMap;
	public HashMap<Points, Double> minimumSchemaEdgeMap;
	public Map<Points,Map<String,Boolean>> yenFlagMap;
	Map<List<Node>,Double> pathLengthOnSchemaMap;
	List<Path> hypoShortestPathsList;
	Map<Integer, Node> existingNodeMap=new ConcurrentHashMap<>();
	public SchemaGraph()
	{
		this.schemaGraph=new HashMap<Node,List<Node>>();
		this.schemaEdgeMap=new HashMap<Points,PriorityQueue<Double>>(); 
		this.minimumSchemaEdgeMap=new HashMap<Points, Double>();
		this.pathLengthOnSchemaMap=new HashMap<List<Node>,Double>();
		this.hypoShortestPathsList=new LinkedList<Path>();
		this.yenFlagMap=new ConcurrentHashMap<>();
	}
	public void


	updateSchemaGraph(List<Map<Points, Double>> updateHypoLBPathList)  // add the length of hypothetical lower bound paths between boundary nodes into the edge map of the skeleton graph, the edge map include several lengths of a pair of boundary nodes
	{
//		for(Points point:this.schemaEdgeMap.keySet()) // clear the old edge weights of schemaEdgeMap
//		{
//			this.schemaEdgeMap.get(point).clear();
//		}

//---------------------------2019.10.13
		for(Points point:schemaEdgeMap.keySet())
		{
			this.schemaEdgeMap.get(point).clear();
		}
//---------------------------2019.10.13

		for(Map<Points, Double> hypoLBPathMap:updateHypoLBPathList)
		{
			for(Points point:hypoLBPathMap.keySet())
			{
				Points versePoint=new Points(point.tnd,point.snd);
				this.schemaEdgeMap.get(point).add(hypoLBPathMap.get(point));
				this.schemaEdgeMap.get(versePoint).add(hypoLBPathMap.get(point));
			}
		}
		generateMinimumEdgeMap(this.schemaEdgeMap);
	}
	public void buildSchemaGraph(List<Map<Points,Double>> originalLowerBoundPathList)
	{
		//Map<Integer, Node> existingNodeMap=new ConcurrentHashMap<>();
		for(Map<Points, Double> hypoLowerBoundPathMap:originalLowerBoundPathList)
		{
			for(Points point:hypoLowerBoundPathMap.keySet())
			{
				try
				{
				Node startNode=null;
				Node secondNode=null;
				if(existingNodeMap.keySet().contains(point.snd))
				{
					startNode=existingNodeMap.get(point.snd);
				}
				else {
					startNode = new Node(point.snd);
					existingNodeMap.put(point.snd, startNode);
				}
				if(existingNodeMap.keySet().contains(point.tnd))
				{
					secondNode=existingNodeMap.get(point.tnd);
				}
				else{
					secondNode=new Node(point.tnd);
					existingNodeMap.put(point.tnd, secondNode);
				}
//				Node startNode=new Node(point.snd);
//				Node secondNode=new Node(point.tnd);
				if(this.schemaGraph.containsKey(startNode))
				{
					if(this.schemaGraph.get(startNode).contains(secondNode)==false)
					{
						this.schemaGraph.get(startNode).add(secondNode);
					}
				}
				else
				{
					List<Node> adjNodeList=new ArrayList<>();
					adjNodeList.add(secondNode);
					this.schemaGraph.put(startNode, adjNodeList);
				}
				
				if(this.schemaGraph.containsKey(secondNode))
				{
				    if(this.schemaGraph.get(secondNode).contains(startNode)==false) {
                        this.schemaGraph.get(secondNode).add(startNode);
                    }
				}
				else
				{
					List<Node> adjNodeList=new ArrayList<>();
					adjNodeList.add(startNode);
					this.schemaGraph.put(secondNode, adjNodeList);
				}
				if(schemaEdgeMap.containsKey(point))
				{
					schemaEdgeMap.get(point).offer(hypoLowerBoundPathMap.get(point));
				}
				else
				{
					PriorityQueue<Double> lenthQueue=new PriorityQueue<Double>();
					lenthQueue.offer(hypoLowerBoundPathMap.get(point));
					schemaEdgeMap.put(point,lenthQueue);
				}
				Points versePoint=new Points(point.tnd,point.snd);
				if(schemaEdgeMap.containsKey(versePoint))
				{
					schemaEdgeMap.get(versePoint).offer(hypoLowerBoundPathMap.get(point));
				}
				else
				{
					PriorityQueue<Double> lenthQueue=new PriorityQueue<Double>();
					lenthQueue.offer(hypoLowerBoundPathMap.get(point));
					schemaEdgeMap.put(versePoint,lenthQueue);
				}
				}
				catch(Exception e)
				{
					System.out.println("There is an exception!");
				}
			}
		}
		//LOG.info("schemaEdgeMap size: {}",schemaEdgeMap.keySet().size());
		//System.out.println("schema graph size:"+this.schemaGraph.keySet().size());
		generateMinimumEdgeMap(this.schemaEdgeMap);
		//this.schemaGraphList=transferSchemaGraph(this.schemaGraph);
	}
	public void addQNToBNEdges(Map<Points,Double> queryNodeToBNEdgeMap)// add queryNodeToBNEdgeMap into schemagraph and edgeMap
	{
		List<Node> queryNodeAdjNodeList=new ArrayList<>();
		int count=0;
		Node queryNode=null;
		for(Points qbPoint:queryNodeToBNEdgeMap.keySet())
		{
			if(count==0)
			{
				queryNode=new Node(qbPoint.snd);
				count++;
			}
			Node adjNode=existingNodeMap.get(qbPoint.tnd);
			queryNodeAdjNodeList.add(adjNode);
			this.schemaGraph.get(adjNode).add(queryNode);
			PriorityQueue<Double> weightQueue=new PriorityQueue();
			weightQueue.add(queryNodeToBNEdgeMap.get(qbPoint));
			this.schemaEdgeMap.put(qbPoint,weightQueue);
			Points reversePoint=new Points(qbPoint.tnd,qbPoint.snd);
			this.minimumSchemaEdgeMap.put(reversePoint, queryNodeToBNEdgeMap.get(qbPoint));
			this.schemaEdgeMap.put(reversePoint,weightQueue);
		}
		this.schemaGraph.put(queryNode, queryNodeAdjNodeList);
		this.minimumSchemaEdgeMap.putAll(queryNodeToBNEdgeMap);

	}
	public void generateMinimumEdgeMap(HashMap<Points,PriorityQueue<Double>> schemaEdgeMap)
	{
		LOG.info("schemaEdgeMap size:{}", schemaEdgeMap.keySet().size());
		for(Points point:schemaEdgeMap.keySet())
		{
			//double originalWeight=minimumSchemaEdgeMap.get(point);
			double newWeight=schemaEdgeMap.get(point).peek();
			this.minimumSchemaEdgeMap.put(point, newWeight);
			//this.yenFlagMap.put(point,false);   //initialize yenFlagMap for the schema graph
		}
	}
//	public HashMap<Node,List<Node>> transferSchemaGraph(HashMap<Node,HashSet<Node>> schemaGraph)
//	{
//		HashMap<Node,List<Node>> schemaGraphList=new HashMap<Node,List<Node>>();
//		for(Node node:schemaGraph.keySet())
//		{
//			List<Node> adjNodeList=new LinkedList<Node>();
//			for(Node adjNode:schemaGraph.get(node))
//			{
//				adjNodeList.add(adjNode);
//			}
//			schemaGraphList.put(node, adjNodeList);
//		}
//		return schemaGraphList;
//	}
//	public List<List<Node>> computeKSPOnSchemaGraph(int h, Node startNode, Node endNode,HashMap<Node,HashSet<Node>> schemaGraph, HashMap<Points, Double> minimumSchemaEdgeMap)
//	{
//		HashMap<Node,List<Node>> schemaGraphList=transferSchemaGraph(schemaGraph);
//		KSPYenRateGraph kspYen=new KSPYenRateGraph();
//		List<List<Node>> KSPonSchemaList=new LinkedList<List<Node>>();
//		KSPonSchemaList=kspYen.kspYenWithoutLoops("aa",h, startNode.id, endNode.id,schemaGraphList, minimumSchemaEdgeMap,yenFlagMap);
//		System.out.println("KSP on schema graph is found!");
//		return KSPonSchemaList;
//	}
//	public List<Path> gethypoShortestPathsList(List<List<Node>> KSPonSchemaList, HashMap<Points, Double> minimumSchemaEdgeMap)
//	{
//		for(List<Node> path:KSPonSchemaList)
//		{
//			double length=0.0;
//			for(int i=0;i<(path.size()-1);i++)
//			{
//				Node firstNode=path.get(i);
//				Node secondNode=path.get(i+1);
//				Points point =new Points(firstNode.id,secondNode.id);
//				length=length+minimumSchemaEdgeMap.get(point);
//			}
//			Path hypoPath=new Path(path, length);
//			this.hypoShortestPathsList.add(hypoPath);
//		}
//		return this.hypoShortestPathsList;
//	}
	public SchemaGraph copySchemaGraph()
	{
		SchemaGraph copySchemaGraph=new SchemaGraph();
		copySchemaGraph.schemaGraph=duplicateGraph(this.schemaGraph);
		copySchemaGraph.minimumSchemaEdgeMap=duplicateMinimumEdgeMap(this.minimumSchemaEdgeMap);
		//copySchemaGraph.yenFlagMap=new ConcurrentHashMap<>();
		return copySchemaGraph;
	}
	public HashMap<Points,Double> duplicateMinimumEdgeMap(HashMap<Points,Double> edgeMap)
	{
		HashMap<Points,Double> copyMinimumEdgeMap=new HashMap<>();
		for(Points point: edgeMap.keySet())
		{
			double weight=edgeMap.get(point);
			copyMinimumEdgeMap.put(point,weight);
		}
		return copyMinimumEdgeMap;
	}
	public HashMap<Node, List<Node>> duplicateGraph (HashMap<Node, List<Node>> graph)//, TimeThread tt)
	{
		HashMap<Node, List<Node>> copyGraph=new HashMap<Node, List<Node>>();
		//Iterator<Node> iterator=graph.keySet().iterator();
		Map<Integer,Node> existingNodeMap=new HashMap<>();
		for(Node keyNode:graph.keySet())
		{
			Node kNode;
			if(existingNodeMap.keySet().contains(keyNode.id))
			{
				kNode=existingNodeMap.get(keyNode.id);
			}
			else
			{
				kNode =(Node)keyNode.clone();
				existingNodeMap.put(kNode.id,kNode);
			}
			List<Node> nodeList=new LinkedList<Node> ();
			for(int i=0;i<graph.get(keyNode).size();i++)
			{
				if(existingNodeMap.keySet().contains(graph.get(keyNode).get(i).id))
				{
					Node adjNode=existingNodeMap.get(graph.get(keyNode).get(i).id);
					nodeList.add(adjNode);
				}
				else
				{
					Node adjNode=(Node)graph.get(keyNode).get(i).clone();
					existingNodeMap.put(adjNode.id,adjNode);
					nodeList.add(adjNode);
				}
			}
			copyGraph.put(kNode, nodeList);
		}
		return copyGraph;
	}
}
