package KSPOnStorm.VirtualHopsBolt;

import KSPOnStorm.*;

import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import KSPOnStorm.VirtualHopsData.VirtualHopsParameter;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualHopsSubgraphBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsSubgraphBolt.class);

    OutputCollector collector;

    List<SubGraph> subgraphList=new ArrayList<SubGraph>();
    HashSet<Integer> subgraphIDset=new HashSet<Integer>();
    Map<Integer,Map<Points,List<Path>>> partialKSPQueryMap;
    //Map<Integer,List<Points>> updatedEdgeToSubgraphMap;
    //Map<Points,Boolean> yenFlagMap;
    List<Integer> QueryProcessingBoltTaskID;
    int scount=0;
    String subBoltID;
    boolean subgraphSendFlag=true;
    long partitionTime=0;
    int label=101;
    long updateSubgraphStartTime=0;
    Map<Integer,Long> queryProcessingTimeMap=new HashMap<>();
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        subBoltID=context.getThisComponentId() + context.getThisTaskId();
        QueryProcessingBoltTaskID=context.getComponentTasks("VirtualHopsQueryProcessingBolt");
        partialKSPQueryMap=new HashMap<>();
    }
    @Override
    public void execute(Tuple input)
    {

        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.SUBGRAPH_MAP))    //receive a subgraph, construct and keep this subgraph and emit it to VirtualHopsQueryProcessingBolts
        {
            SubGraph sg=new SubGraph();
            Partition par=((Partition) input.getValueByField(VirtualHopStreamFields.Subgraph));

            try {
                constructSubGraph(sg,par);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            subgraphIDset.add(par.partitionId);
            subgraphList.add(sg);

            if(sg.boudaryNodeList.size()>1) {
                this.collector.emit(VirtualHopStreamIDs.boundPathSubgraph, new Values(sg.hypotheticalPathMap));
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.subGraphAssignFlag))              //All subgraphs have been sent from ConstructGraphBolt to VirtualHopsSubGraphBolts
        {
            this.partitionTime=(long)input.getValueByField(VirtualHopStreamFields.startTime);
            this.collector.emit(VirtualHopStreamIDs.subGraphSendFlag,new Values(subgraphSendFlag,this.partitionTime));   //This VirtualHopsSubGraphBolt has sent its all subgraphs to the VirtualHopsConstructSchemaGraphBolt
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.OneHypotheticalPath))      //receive a hypoPath, compute the partial paths between any two adjacent nodes in this path
        {
            long startProcessingTime=System.currentTimeMillis();
            String labelString=subBoltID+"+"+label;

            boolean flag=(Boolean) input.getValueByField(VirtualHopStreamFields.FilteredPointsFlag);
            if(flag==false)
            {
                Path hypoPath=(Path) input.getValueByField(VirtualHopStreamFields.HypotheticalPath);
                int queryID=(int) input.getValueByField(VirtualHopStreamFields.QueryIDLableHypoPath);
                int queryBoltTaskID=(int) input.getValueByField(VirtualHopStreamFields.QueryProcessingBoltTaskID);
                //System.out.println("Subgraph Bolt: receive "+hypoPath.hypoID+"th hypoPath!");
                Map<Points, List<Path>> partialKSP= null;
                try {
                    partialKSP = computeKShortestPathsBasedOnHPath(VirtualHopsParameter.queryK, hypoPath);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                long endProcessingTime=System.currentTimeMillis();
                long usedTime=endProcessingTime-startProcessingTime;
                collector.emitDirect(queryBoltTaskID, VirtualHopStreamIDs.KPartialShortestPaths,new Values(queryID,partialKSP,labelString,usedTime));
                label++;
            }
            else
            {
                Set<Points> filterPointsSet=(Set<Points>)input.getValueByField(VirtualHopStreamFields.HypotheticalPath);
                int queryID=(int) input.getValueByField(VirtualHopStreamFields.QueryIDLableHypoPath);
                int queryBoltTaskID=(int) input.getValueByField(VirtualHopStreamFields.QueryProcessingBoltTaskID);
                Map<Points, List<Path>> partialKSP= null;
                try {
                    partialKSP = filterPointsForComputeKShortestPaths(VirtualHopsParameter.queryK, filterPointsSet);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                long endProcessingTime=System.currentTimeMillis();
                long usedTime=endProcessingTime-startProcessingTime;
                collector.emitDirect(queryBoltTaskID, VirtualHopStreamIDs.KPartialShortestPaths,new Values(queryID,partialKSP,labelString,usedTime));
                label++;
            }
            //collector.emit(VirtualHopStreamIDs.KPartialShortestPaths,new Values(partialKSP));  //emit the map to the SchemaGraphBolt
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdateEdgePoints))
        {
            if(this.updateSubgraphStartTime==0)
            {
                this.updateSubgraphStartTime=System.currentTimeMillis();
                collector.emit(VirtualHopStreamIDs.UpdateSubgraphStartTime, new Values(this.updateSubgraphStartTime));
            }
            Points point=(Points)input.getValueByField(VirtualHopStreamFields.UpdateEdgePoint);
            for(SubGraph sg:this.subgraphList)
            {
                if(sg.subEdgeSet.keySet().contains(point))
                {
                    sg.updateWeight(point);
//                    if(this.updatedEdgeToSubgraphMap.keySet().contains(sg.subgraphID))  //keep the upated edges to update the edgeMap of subgraphs
//                    {
//                        this.updatedEdgeToSubgraphMap.get(sg.subgraphID).add(point);
//                    }
//                    else
//                    {
//                        List<Points> edgeList=new ArrayList<>();
//                        edgeList.add(point);
//                        this.updatedEdgeToSubgraphMap.put(sg.subgraphID,edgeList);
//                    }
                }
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdateGraphSendFinishFlag)) // which means construct graph bolt finishes the updating graph
        {
         //   System.out.println("Receive graph send finish flag");
            for(SubGraph sg:this.subgraphList)
            {
                sg.updateLowerBoundPaths();
                sg.updateEdgeOfSubgraph();
                collector.emit(VirtualHopStreamIDs.UpdatedLowerBoundPaths, new Values(sg.hypotheticalPathMap));
            }
            collector.emit(VirtualHopStreamIDs.SubgraphSendUpdatedLBPathFinish,new Values(true));        //this means this subgraph bolt finishes the updating of hypothetical paths for the subgraphs charged by itself

//            new note code
//            for(SubGraph sg:subgraphList)
//            {
//                sg.equalEdgeSet.clear();
//                sg.hypotheticalPathMap.clear();
//                sg.lowerBoundPath.clear();
//            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ModifiedQueryToSubgraphBolt))  //find the hypo paths from queryNode to all boundary nodes in every subgraph
        {
            //LOG.info("{} subgraph bolt receives the modified query", this.subBoltID);
            Query query=(Query) input.getValueByField(VirtualHopStreamFields.ModifiedQueryToSubgraphField);
            //long queryGenerateTime=(long) input.getValueByField(VirtualHopStreamFields.ModifiedQueryToSubgraphFieldTime);
            //long subgraphBoltTime=System.currentTimeMillis();
            //long comTimeFormQueryboltToSubgraphbolt=subgraphBoltTime-queryGenerateTime;
            //LOG.info("comTimeFormQueryboltToSubgraphbolt: {}",comTimeFormQueryboltToSubgraphbolt);
            Node queryFirstNode=new Node(query.sourceNodeID);
            Node querySecondNode=new Node(query.endNodeID);
            for(int i=0;i<subgraphList.size();i++)
            {
                SubGraph sg=subgraphList.get(i);
                Set<Node> boundNodeSet=new ConcurrentHashSet<>();
                boundNodeSet.addAll(sg.boudaryNodeList);

                if(sg.subGraph.keySet().contains(queryFirstNode)&&!boundNodeSet.contains(queryFirstNode))
                {
                    LOG.info("Subgraph ID: {}: find the first query node ID: {}.",sg.subgraphID,query.sourceNodeID);
                    //sg.boudaryNodeList.add(queryFirstNode);
                    Map<Points,Double> queryNodeToBNEdgeFMap= null;
                    try {
                        queryNodeToBNEdgeFMap = getQnToBnEdgeMap(sg,String.valueOf(sg.subgraphID),query.sourceNodeID ,sg.boudaryNodeList ,sg.subGraph , sg.subEdgeSet,sg.yenFlagMap);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    sg.boudaryNodeList.add(queryFirstNode);
                    collector.emit(VirtualHopStreamIDs.ModifiedQueryNodeToBoundNodeEdges, new Values(queryNodeToBNEdgeFMap));
                }
                if(sg.subGraph.keySet().contains(querySecondNode)&&!boundNodeSet.contains(querySecondNode))
                {

                   // Map<Points,Double> queryNodeToBNEdgeMap=new ConcurrentHashMap<>();
                    LOG.info("Subgraph ID: {}: find the second query node ID: {}.",sg.subgraphID, query.endNodeID);
                    Map<Points,Double> queryNodeToBNEdgeSMap= null;
                    try {
                        queryNodeToBNEdgeSMap = getQnToBnEdgeMap(sg, String.valueOf(sg.subgraphID),query.endNodeID ,sg.boudaryNodeList ,sg.subGraph, sg.subEdgeSet,sg.yenFlagMap);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    sg.boudaryNodeList.add(querySecondNode);
                    collector.emit(VirtualHopStreamIDs.ModifiedQueryNodeToBoundNodeEdges, new Values(queryNodeToBNEdgeSMap));
                }
            }
            long addQueryToSkeletonGraphTime=System.currentTimeMillis();
            collector.emit(VirtualHopStreamIDs.ModifiedQueryEdgesSendFinish, new Values(true));
            //LOG.info("{} subgraph bolt finished processing query node to boundary node", this.subBoltID);
        }
    }

    public Map<Points,Double> getQnToBnEdgeMap(SubGraph sg, String subgraphBoltID, int queryNodeID,List<Node> boundNodeList,Map<Node, List<Node>> subGraph,Map<Points,Double> subEdgeSet,Map<Points,Map<String,Boolean>> yenFlagMap) throws InterruptedException {
        KSPYenparallelGraph kspRG=new KSPYenparallelGraph();
        Map<Points,List<List<Node>>> lowerBoundPath=new ConcurrentHashMap<>();
        Map<Points,Double> qNToBnEdgeMap=new ConcurrentHashMap<>();
        for(int j=0;j<boundNodeList.size();j++)
        {
            Node node=boundNodeList.get(j);
            Points point=new Points(queryNodeID,node.id);
            List<List<Node>> lbPath =kspRG.kspYenWithoutLoops(subgraphBoltID, VirtualHopsParameter.BP_X,queryNodeID, node.id, subGraph, subEdgeSet, yenFlagMap);
            lowerBoundPath.put(point,lbPath);
            sg.lowerBoundPath.put(point, lbPath);
        }
        for(Points point:lowerBoundPath.keySet())
        {
            double minRealLength=Double.MAX_VALUE;
            List<List<Node>> paths=lowerBoundPath.get(point);
            for(List<Node>path:paths)
            {
                double realLength=realPathLength(path,subEdgeSet);
                if(minRealLength>realLength)
                {
                    minRealLength=realLength;
                }
            }
            double lowerboundLength=minRealLength;

            if(minRealLength>lowerboundLength)
            {
                qNToBnEdgeMap.put(point, lowerboundLength);
                sg.hypotheticalPathMap.put(point,lowerboundLength);
            }
            else
            {
                qNToBnEdgeMap.put(point, minRealLength);
                sg.hypotheticalPathMap.put(point,minRealLength);
            }
        }
        return qNToBnEdgeMap;
    }
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
    public void constructSubGraph(SubGraph sg, Partition par) throws InterruptedException {
        if(subgraphIDset.contains(par.partitionId))
        {
        }
        else
        {
            sg.subGraph=duplicateGraph(par.subGraph);
            sg.subgraphID=par.partitionId;
            sg.boudaryNodeList=par.boundaryNodeList;
            sg.subgraphID=par.partitionId;
            // System.out.println("The "+par.partitionId+"th boundary node size: "+par.boundaryNodeList.size());
            sg.subEdgeSet=par.subEdgeMap;
            sg.oldCopysubEdgeSet=par.subEdgeMap;
            //sg.equalEdgeSet=sg.buildEqualEdgeSet(sg.subGraph);

            if(sg.boudaryNodeList.size()>1)
            {
                sg.computeLowerBoundPath(this.subBoltID);
                sg.computeHypotheticalPathLength();
            }
        }
    }
    public Map<Points, List<Path>> computeKShortestPathsBasedOnHPath(int k, Path hypotheticalPath) throws InterruptedException        // compute k shortest paths based on the given hypothetical paths
    {
        Map<Points, List<Path>> PathsBetweenTwoPointsMap = new HashMap<Points, List<Path>>();
        KSPYenparallelGraph kspYen = new KSPYenparallelGraph();
        for (int i = 0; i < (hypotheticalPath.line.size() - 1); i++) {
            Node firstNode = hypotheticalPath.line.get(i);
            Node secondNode = hypotheticalPath.line.get(i + 1);
            for (SubGraph sg : this.subgraphList)
            {
                if (sg.boudaryNodeList.contains(firstNode) && sg.boudaryNodeList.contains(secondNode))
                {
                    Points point = new Points(firstNode.id, secondNode.id);
                    List<List<Node>> partShortestPaths = kspYen.kspYenWithoutLoops(subBoltID,k, firstNode.id, secondNode.id, sg.subGraph, sg.subEdgeSet,sg.yenFlagMap);
                    List<Path> subPathList = new ArrayList<Path>();
                    if(partShortestPaths==null)
                    {
                        //System.out.println("SubgraphBolt: no partial KSP between "+firstNode.id+" and "+secondNode.id);
                        LOG.info("SubgraphBolt: no partial KSP between {} and {}!!!!!!!!!!",firstNode.id,secondNode.id);
                    }
                    else
                    {
                        for (List<Node> subpath : partShortestPaths) {
                            double length = computePathLength(subpath, sg.subEdgeSet);
                            Path subRealPath = new Path(subpath, length);
                            subPathList.add(subRealPath);
                        }
                    }
                    if(PathsBetweenTwoPointsMap.isEmpty())
                    {
                        Collections.sort(subPathList);
                        PathsBetweenTwoPointsMap.put(point,subPathList);
                        //System.out.println(subBoltID+": point "+point.snd+", "+point.tnd+" has put partial ksp!");
                    }
                    else
                    {
                        if(PathsBetweenTwoPointsMap.keySet().contains(point))
                        {
                            List<Path> preSubPathList=PathsBetweenTwoPointsMap.get(point);
                            for(Path subPath:subPathList)
                            {
                                if(!preSubPathList.contains(subPath))
                                {
                                    preSubPathList.add(subPath);
                                }
                            }
                            PathsBetweenTwoPointsMap.put(point,preSubPathList);
                        }
                        else
                        {
                            PathsBetweenTwoPointsMap.put(point, subPathList);
                        }
                    }
                    //This map keeps all shortest paths between the two adjacent nodes in the given hypothetical path
                }
            }
        }
        return PathsBetweenTwoPointsMap;
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
            List<Node> nodeList=new ArrayList<Node> ();
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
    public double computePathLength(List<Node> path,Map<Points,Double> subEdgeSet)
    {
        double length=0.0;
        for(int i=0;i<(path.size()-1);i++)
        {
            Node startNode=path.get(i);
            Node endNode=path.get(i+1);
            Points point=new Points(startNode.id,endNode.id);
            length=length+subEdgeSet.get(point);
        }
        return length;
    }

    public Map<Points, List<Path>> filterPointsForComputeKShortestPaths(int k, Set<Points> filterPointsSet) throws InterruptedException        // compute k shortest paths based on the given hypothetical paths
    {
        Map<Points, List<Path>> PathsBetweenTwoPointsMap = new HashMap<Points, List<Path>>();
        KSPYenparallelGraph kspYen = new KSPYenparallelGraph();
        for (Points filPoint:filterPointsSet) {
            Node firstNode = new Node(filPoint.snd);
            Node secondNode = new Node(filPoint.tnd);
            for (SubGraph sg : this.subgraphList)
            {
                if (sg.boudaryNodeList.contains(firstNode) && sg.boudaryNodeList.contains(secondNode))
                {
                    Points point = new Points(firstNode.id, secondNode.id);
                    List<List<Node>> partShortestPaths = kspYen.kspYenWithoutLoops(subBoltID,k, firstNode.id, secondNode.id, sg.subGraph, sg.subEdgeSet,sg.yenFlagMap);
                    List<Path> subPathList = new ArrayList<Path>();
                    if(partShortestPaths==null)
                    {
                        //System.out.println("SubgraphBolt: no partial KSP between "+firstNode.id+" and "+secondNode.id);
                        LOG.info("SubgraphBolt: no partial KSP between {} and {}",firstNode.id,secondNode.id);
                    }
                    else
                    {
                        for (List<Node> subpath : partShortestPaths) {
                            double length = computePathLength(subpath, sg.subEdgeSet);
                            Path subRealPath = new Path(subpath, length);
                            subPathList.add(subRealPath);
                        }
                    }
                    if(PathsBetweenTwoPointsMap.isEmpty())
                    {
                        Collections.sort(subPathList);
                        PathsBetweenTwoPointsMap.put(point,subPathList);
                        //System.out.println(subBoltID+": point "+point.snd+", "+point.tnd+" has put partial ksp!");
                    }
                    else
                    {
                        if(PathsBetweenTwoPointsMap.keySet().contains(point))
                        {
                            List<Path> preSubPathList=PathsBetweenTwoPointsMap.get(point);
                            for(Path subPath:subPathList)
                            {
                                if(!preSubPathList.contains(subPath))
                                {
                                    preSubPathList.add(subPath);
                                }
                            }
                            PathsBetweenTwoPointsMap.put(point,preSubPathList);
                            // System.out.println(subBoltID+": point "+point.snd+", "+point.tnd+" has put partial ksp!");
                        }
                        else
                        {
                            PathsBetweenTwoPointsMap.put(point, subPathList);
                        }
                    }
                    //This map keeps all shortest paths between the two adjacent nodes in the given hypothetical path
                }
            }
        }
        return PathsBetweenTwoPointsMap;
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(VirtualHopStreamIDs.boundPathSubgraph,
                new Fields(VirtualHopStreamFields.LowerBoundPathSubgraph));

        declarer.declareStream(VirtualHopStreamIDs.subGraphSendFlag,
                new Fields(VirtualHopStreamFields.SubGraphSend_Flag,VirtualHopStreamFields.startTime));

        declarer.declareStream(VirtualHopStreamIDs.KPartialShortestPaths, true,
                new Fields(VirtualHopStreamFields.QueryIDReturnedBySubgraphBolt,VirtualHopStreamFields.PartitialKShortestPaths,VirtualHopStreamFields.labelCount,VirtualHopStreamFields.timeForHypoPathSubGrphBolt));

        declarer.declareStream(VirtualHopStreamIDs.UpdatedLowerBoundPaths,
                new Fields(VirtualHopStreamFields.UpdateLowerBoundPaths));

        declarer.declareStream(VirtualHopStreamIDs.SubgraphSendUpdatedLBPathFinish,
                new Fields(VirtualHopStreamFields.SubgraphSendUpdatedLBPathFinishField));

        declarer.declareStream(VirtualHopStreamIDs.UpdateSubgraphStartTime,
                new Fields(VirtualHopStreamFields.UpdateSubgraphStartTimeField));

        declarer.declareStream(VirtualHopStreamIDs.ModifiedQueryNodeToBoundNodeEdges,
                new Fields(VirtualHopStreamFields.ModifiedQueryNodeToBoundNodeEdgesField));

        declarer.declareStream(VirtualHopStreamIDs.ModifiedQueryEdgesSendFinish,
                new Fields(VirtualHopStreamFields.ModifiedQueryEdgesSendFinishField));
    }

}
