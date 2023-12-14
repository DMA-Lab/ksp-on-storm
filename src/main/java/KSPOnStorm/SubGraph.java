package KSPOnStorm;

import KSPOnStorm.VirtualHopsData.VirtualHopsParameter;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//import java.util.Random;

public class SubGraph implements Serializable {

	public Map<Node, List<Node>> subGraph;
    public Map<Points,Double> subEdgeSet;
	public Map<Points,Double> oldCopysubEdgeSet;
    public Map<Points,UnitLength> updatedEdgeMap;
	//public Map<Points,Double> equalEdgeSet;
	public Map<Points,Double> hypotheticalPathMap;
 //   HashMap<Points,HashMap<Integer, Integer>> segmentEdgeMap;
    public List<Node> boudaryNodeList;
	public Map<Points,List<List<Node>>> lowerBoundPath;
	public Map<Points,Map<String,Boolean>> yenFlagMap;
  //  public int k=2;
    public int subgraphID;
    public SubGraph()
	{
		this.lowerBoundPath=new HashMap<>();
		//this.subgraphID=subgraphID;
		this.subEdgeSet=new HashMap<>();
		this.oldCopysubEdgeSet=new ConcurrentHashMap<>();
		this.hypotheticalPathMap=new ConcurrentHashMap<>();
		this.yenFlagMap=new HashMap<>();
		this.updatedEdgeMap=new HashMap<>();
	}
    public SubGraph(Map<Node, List<Node>> subGraph, List<Node>boudaryNodeList, int subgraphID, HashMap<Points,Double> wholeEdgeSet)
    {
    	this.subGraph=subGraph;
    	this.boudaryNodeList=boudaryNodeList;
    	//this.lowerBoundPath=new HashMap<Points,List<List<Node>>>();
		this.lowerBoundPath=new HashMap<>();
    	this.subgraphID=subgraphID;
    	this.subEdgeSet=new HashMap<>();
		this.oldCopysubEdgeSet=new ConcurrentHashMap<>();
    	this.hypotheticalPathMap=new ConcurrentHashMap<>();
    	//buildEqualEdgeSet(this.subGraph);
    	this.buildSubEdgeSet(this.subGraph,wholeEdgeSet);
		this.yenFlagMap=new HashMap<>();
    	//PrintFunctions.printEdgeSet(this.subEdgeSet);
    }
	public void updateWeight(Points point)
	{
		double originalWeight=this.oldCopysubEdgeSet.get(point);
		double newWeight;
		Random rand=new Random();
		//double varyRate = (rand.nextInt(5000) - 5000) / 10000.0;
		double varyRate = (rand.nextInt(5000) - 5000) / 10000.0;

		newWeight=originalWeight*(1+varyRate);
		int unitNumber=this.oldCopysubEdgeSet.get(point).intValue();
		UnitLength ul=new UnitLength(newWeight,unitNumber);
		//System.out.println("Points: "+point.snd+", "+point.tnd);
		//ul.outPutString();
		Points versePoint=new Points(point.tnd,point.snd);
		this.subEdgeSet.put(point,newWeight);
		this.subEdgeSet.put(versePoint,newWeight);
		this.updatedEdgeMap.put(point,ul);
		//update edges of subgraph 2019.10.13
		//this.subEdgeSet.put(point,newWeight);
		//this.subEdgeSet.put(inversePoint,newWeight);

	}
	public void updateLowerBoundPaths()     //update the lower bound paths between any pair of boundary nodes of a subgraph after receiving varying weights of edges
	{
		Queue<UnitLength> ulQueue=new PriorityQueue<>();
		ulQueue.addAll(this.updatedEdgeMap.values());
		for(Points point: this.lowerBoundPath.keySet())
		{
			List<List<Node>> lowerBoundPathList=this.lowerBoundPath.get(point);
			Queue<Double> updatedPathWeightQueue=new PriorityQueue<>();
			Queue<Double> realPathWeightQueue=new PriorityQueue<>();
			for(List<Node> path:lowerBoundPathList)
			{
				double updatedpathWeight=0;                    // the summary of weights for a path
				double realPathWeight=0;
				for(int j=0;j<(path.size()-1);j++)
				{
					int snId=path.get(j).id;
					int tnId=path.get(j+1).id;
					Points everyPoint=new Points(snId,tnId);
					Points inversePoint=new Points(everyPoint.tnd,everyPoint.snd);
					if(this.updatedEdgeMap.containsKey(everyPoint)||this.updatedEdgeMap.containsKey(inversePoint))
					{
						if(this.updatedEdgeMap.containsKey(everyPoint))
						{
							realPathWeight=realPathWeight+this.updatedEdgeMap.get(everyPoint).newWeight;
						}
						else
						{
							realPathWeight=realPathWeight+this.updatedEdgeMap.get(inversePoint).newWeight;
						}
					}
					else
					{
						realPathWeight=realPathWeight+this.subEdgeSet.get(everyPoint);
					}
					double updatedEdgeWeight=0;
					int unitNumber=this.subEdgeSet.get(everyPoint).intValue();
					//System.out.println("unitNumber: "+unitNumber);
					while (unitNumber>0) //computing the updated weight for en edge
					{
						UnitLength ul;
						if(!ulQueue.isEmpty())
						{
							ul=ulQueue.poll();
						}
						else
						{
							ul=new UnitLength(1000000,1000000);
						}
					//	System.out.println("Minimum unit average weight: "+ ul.unitAverageWeight);
						if(ul.unitNumber>unitNumber||ul.unitNumber==unitNumber)
						{
							updatedEdgeWeight=updatedEdgeWeight+ul.unitAverageWeight*unitNumber;
							unitNumber=unitNumber-ul.unitNumber;
						}
						else
						{
							updatedEdgeWeight=updatedEdgeWeight+ul.unitAverageWeight*ul.unitNumber;
							unitNumber=unitNumber-ul.unitNumber;
						}
					}
					updatedpathWeight=updatedpathWeight+updatedEdgeWeight;

//					if(originPathWeight!=pathWeight)
//					{
//						System.out.println("Subgraph: updated path weight:"+pathWeight);
//						System.out.println("Subgraph: original path weight:"+originPathWeight);
//					}
				}
				updatedPathWeightQueue.add(updatedpathWeight);
				realPathWeightQueue.add(realPathWeight);
			}
			double firstShortestDis=realPathWeightQueue.poll();
			double lowerboundDistance=firstShortestDis;
			while(!updatedPathWeightQueue.isEmpty())
				{
					double updatedBound=updatedPathWeightQueue.poll();
					if(updatedBound<firstShortestDis)
					{
						lowerboundDistance=updatedBound;
					}
					else
					{
						lowerboundDistance=firstShortestDis;
					}
				}
			hypotheticalPathMap.put(point,lowerboundDistance);
		}
	}
	public void updateEdgeOfSubgraph()
	{
		for(Points point:this.updatedEdgeMap.keySet())
		{
			UnitLength ul=this.updatedEdgeMap.get(point);
			this.subEdgeSet.put(point,ul.newWeight);
			Points versePoint=new Points(point.tnd,point.snd);
			this.subEdgeSet.put(versePoint,ul.newWeight);
		}
	}
//    public Map<Points,Double> buildEqualEdgeSet(Map<Node, List<Node>> subGraph)
//    {
//    	this.equalEdgeSet=new HashMap<>();
//
//		for(Node keyNode:subGraph.keySet())
//		{
//			for(Node adjNode:subGraph.get(keyNode))
//			{
//				Points point=new Points(keyNode.id,adjNode.id);
//				this.equalEdgeSet.put(point, 1.0);
//				//Points point1=new Points(keyNode.id,adjNode.id);
//				//this.yenFlagMap.put(point1,false);
//			}
//		}
//		return this.equalEdgeSet;
//    }
 /*   public void buildSegmentEdgeMap()
    {
    	for(Points point:this.subEdgeSet.keySet())
    	{
    		int edgeLenth=subEdgeSet.get(point);
    		int segNumber=(int)Math.ceil(edgeLenth/100);
    		HashMap<Integer, Integer> lengthPair=new HashMap<Integer, Integer>();
    		lengthPair.put(edgeLenth, segNumber);
    		this.segmentEdgeMap.put(point, lengthPair);
    	}
    }*/
    public void buildSubEdgeSet(Map<Node, List<Node>> subGraph,HashMap<Points,Double> edgeSet)
    {
    	Iterator<Node> iterator=subGraph.keySet().iterator();
		while (iterator.hasNext())
		{
			Node keyNode=(Node)iterator.next();
			//System.out.print(keyNode.id+": ");
			for(int i=0;i<subGraph.get(keyNode).size();i++)
			{
				//System.out.print(subGraph.get(keyNode).get(i).id+", ");
				Points point=new Points(keyNode.id, subGraph.get(keyNode).get(i).id);
				this.subEdgeSet.put(point, edgeSet.get(point));
				this.oldCopysubEdgeSet.put(point,edgeSet.get(point));
			}
			//System.out.println();
		}
    }
	
//    public double  hypotheticalPathLength(int hops, HashMap<Points,Double> subEdgeSet)
//    {
//    	double hypoLength=0;
//    	Queue<Double> edgeQueue=new PriorityQueue<Double>();
//    	edgeQueue.addAll(subEdgeSet.values());
//    	for(int i=0;i<hops;i++)
//    	{
//    		hypoLength=hypoLength+edgeQueue.poll();
//    	}
//    	return hypoLength;
//    }
    public double realPathLength(List<Node> path,Map<Points,Double> subEdgeSet)
    {
    	double realLength=0;
    	for(int i=0;i<(path.size()-1);i++)
    	{
    		Node snode=path.get(i);
    		Node tnode=path.get(i+1);
    		Points point=new Points(snode.id, tnode.id); 
    		realLength=realLength+subEdgeSet.get(point);
    	}
    	return realLength;
    }
    public void computeLowerBoundPath(String subgraphBoltID) throws InterruptedException {
    	List<Node> temporaryBNList=new ArrayList<Node>();
    	temporaryBNList.addAll(this.boudaryNodeList);
		KSPYenparallelGraph ksprg=new KSPYenparallelGraph();
    	for(int i=0;i<this.boudaryNodeList.size();i++)
    	{
    		Node node=this.boudaryNodeList.get(i);
    		temporaryBNList.remove(node);
    		for(Node tNode:temporaryBNList)
    		{
    			Points point=new Points(node.id, tNode.id);
    			List<List<Node>> lbPath =ksprg.kspYenWithoutLoops(subgraphBoltID, VirtualHopsParameter.BP_X, node.id, tNode.id,this.subGraph,this.subEdgeSet,this.yenFlagMap);
    			this.lowerBoundPath.put(point, lbPath);
    			//System.out.println("("+node.id+", "+tNode.id+") has been processed");
    		}
    	}
  //  	PrintFunctions.printLowerBoundPaths(this.lowerBoundPath);
    }
    public void computeHypotheticalPathLength()
    { 
    	for(Points point:this.lowerBoundPath.keySet())
    	{
    		double minRealLength=Double.MAX_VALUE;
    		List<List<Node>> paths=this.lowerBoundPath.get(point);
    		for(List<Node>path:paths)
    		{
    			double realLength=realPathLength(path,this.subEdgeSet);
    			if(minRealLength>realLength)
    			{
    				minRealLength=realLength;
    			}
    		}
    		double lowerboundLength=minRealLength;
    		//double lowerboundLength=hypotheticalPathLength(paths.get(paths.size()-1).size(),this.subEdgeSet);
    		//System.out.println("lowerboundLength: "+lowerboundLength);
    		//System.out.println("RealPathLength:"+minRealLength);
			this.hypotheticalPathMap.put(point, lowerboundLength);
//			if(minRealLength>lowerboundLength)
//    		{
//
//    		}
//    		else
//    		{
//    			this.hypotheticalPathMap.put(point, minRealLength);
//    		}
    	}
    }
}
