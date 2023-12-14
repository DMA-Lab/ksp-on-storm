package KSPOnStorm.VirtualHopsBolt;
import KSPOnStorm.*;
import KSPOnStorm.VirtualHopsData.VirtualHopsParameter;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import KSPOnStorm.VirtualHopsData.VirtualHopsBoltNumberConfig;
//import org.apache.storm.shade.org.apache.curator.framework.recipes.leader.LeaderSelector;
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
//import sun.rmi.runtime.Log;
//
//import javax.lang.model.util.ElementScanner6;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.logging.XMLFormatter;


public class VirtualHopsQueryProcessingBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsQueryProcessingBolt.class);

    OutputCollector collector;
    Map<Integer, List<Path>> HypoPathsForQueryMap;
    Map<Integer, Integer> partialPathcountMap;
    Map<Integer,Map<Points,List<Path>>> partialKSPQueryMap;
    Map<Integer,List<Path>> KSPQueryMap;
   // List<SubGraph> subGraphList=new LinkedList<SubGraph>();
    List<Map<Points,Double>> updatedLowerBoundPathsList;
    List<Map<Points,Double>> originalLowerBoundPathList;
   private String boltID;
   private long totalTime;
    int taskID;
    int queryCount;
    int currentQueryTime;
    int subgraphBoltCount=0;
    int sgUpHypoLBPathCount=0;
    SchemaGraph schgra=null;
    SchemaGraph copyschgra=null;
    long startTime=0;
    long endTime=0;
    long updateSkeletonStartTime=0;
    long updateSkeletonEndTime=0;
    long usedTotalMemory=0;
    long usedFreeMemory=0;
    long initialMemory=0;
    long initialFreeMemory=0;
    long initialUsedMemory=0;
    int queryEdgeSubgraphBoltCount=0;
    Map<Integer,Long> queryProcessingTimeMap=new HashMap<>();
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        HypoPathsForQueryMap = new ConcurrentHashMap<>();
        partialPathcountMap = new ConcurrentHashMap<>();
        partialKSPQueryMap=new ConcurrentHashMap<>();
        KSPQueryMap=new ConcurrentHashMap<>();
        taskID = context.getThisTaskId();
        queryCount=0;
        totalTime=0;
        currentQueryTime=0;

        this.boltID=context.getThisComponentId()+context.getThisTaskId();
        schgra=new SchemaGraph();
        updatedLowerBoundPathsList=new ArrayList<>();
        originalLowerBoundPathList=new ArrayList<>();
        initialMemory=Runtime.getRuntime().totalMemory();
        initialFreeMemory=Runtime.getRuntime().freeMemory();
        initialUsedMemory=initialMemory-initialFreeMemory;
        LOG.info("QueryProcessingBolt: initial used memory is {}",initialUsedMemory);
        //   allPartialPathsMap
    }

    @Override
    public void execute(Tuple input) {
        //SchemaGraph copySchemaGraph=null;
        //Map<Points, Map<String, Boolean>> cache = null;
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdateSubgraphStartTime))
        {
            //if(this.updateSkeletonStartTime==0)
            long temporalSTime=(long)input.getValueByField(VirtualHopStreamFields.UpdateSubgraphStartTimeField);
            if(temporalSTime>this.updateSkeletonStartTime)
            {
                this.updateSkeletonStartTime=temporalSTime;
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdatedLowerBoundPaths))  //receive a map of hypothetical lower bound paths between boundary nodes of each subgraph
        {
            Map<Points,Double> sgUpdateHypoPathMap=(Map<Points, Double>)input.getValueByField(VirtualHopStreamFields.UpdateLowerBoundPaths);
            this.updatedLowerBoundPathsList.add(sgUpdateHypoPathMap);
            //System.out.println("Processing query bolt: receive updatedLowerBoundPathsList");
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.SubgraphSendUpdatedLBPathFinish))   //when this queryQrocess bolt receives all updatedHypoLBPaths, it will update the skeleton graph
        {
            sgUpHypoLBPathCount++;
            if(sgUpHypoLBPathCount==VirtualHopsBoltNumberConfig.VirtualHopsSubgraphBoltNumber)
            {
                this.schgra.updateSchemaGraph(this.updatedLowerBoundPathsList);  //the skeleton graph is updated
                LOG.info("ProcessingQueryBolt: {}, need to be updated edges: {}",boltID,this.updatedLowerBoundPathsList.size());

                collector.emit(VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish, new Values(true)); //notify query generate bolt that the skeleton graph has been updated
                this.updateSkeletonEndTime=System.currentTimeMillis();
                long updateTime=this.updateSkeletonEndTime-this.updateSkeletonStartTime;
                LOG.info("Time of updating skeleton graph: {}!!!!!!!!!!!!!!!!!", updateTime);
                this.partialKSPQueryMap.clear();
              //  WriteResultsIntoFile wrf=new WriteResultsIntoFile();
//                wrf.writeResult(updateTime,this.taskID+" the time of updating skeleton is ");
              //  System.out.println("QueryProcessingBolt: update skeleton graph!");
               // PrintFunctions.printEdgeSet(this.schgra.minimumSchemaEdgeMap);
                updatedLowerBoundPathsList.clear();
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.boundPathSubgraph))   //receive a subgraph including lowerbound paths between any pair of boundary nodes
        {
            Map<Points, Double> hypoLowerBounePathMap=(Map<Points, Double>) input.getValueByField(VirtualHopStreamFields.LowerBoundPathSubgraph);
            originalLowerBoundPathList.add(hypoLowerBounePathMap);
//            SubGraph sg=(SubGraph) input.getValueByField(VirtualHopStreamFields.LowerBoundPathSubgraph);
//            subGraphList.add(sg);
            //System.out.println("ShcemaBolt: receive "+this.subGraphList.size()+" subgraphs");
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.subGraphSendFlag))      //receive a flag that indicates a subgraph bolt has sent its all subgrahs
        {
            this.startTime=(long)input.getValueByField(VirtualHopStreamFields.startTime);
            subgraphBoltCount++;
            // System.out.println("SchemaGraph: receive the subgraphs from a subgraphbolt!"+"  The number is "+subgraphBoltCount);

            if(subgraphBoltCount==VirtualHopsBoltNumberConfig.VirtualHopsSubgraphBoltNumber)
            {
                // System.out.println("The number of subgraph: "+this.subGraphList.size());
                this.schgra.buildSchemaGraph(this.originalLowerBoundPathList);               //construct a schema graph
                //System.out.println("The schema graph is built!");
                //this.subGraphList.clear();
                LOG.info("{}: Number of points: {}", taskID, this.schgra.minimumSchemaEdgeMap.keySet().size());
                endTime=System.currentTimeMillis();
                long buildingTime=endTime-this.startTime;
                //System.out.println("Time of building graph: "+(buildingTime));
                LOG.info("{}: Time of building skeleton graph: {}", taskID, buildingTime);
                usedTotalMemory=Runtime.getRuntime().totalMemory();
                usedFreeMemory=Runtime.getRuntime().freeMemory();
                long endUsedMemory=usedTotalMemory-usedFreeMemory;
                long skeletonMemory=endUsedMemory-this.initialUsedMemory;
                LOG.info("QueryProcessingBolt: The used memory after building skeleton graph: {}", endUsedMemory);
                LOG.info("QueryProcessingBolt: Memory for skeleton graph: {} !!!", skeletonMemory);
                originalLowerBoundPathList.clear();

                collector.emit(VirtualHopStreamIDs.SkeletonGraphBuilt,new Values(skeletonMemory)); //send skeleton graph to processQueryBolt
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ModifiedQueryNodeToBoundNodeEdges))
        {
            Map<Points,Double> queryNodeToBNEdgeMap=(Map<Points,Double>)input.getValueByField(VirtualHopStreamFields.ModifiedQueryNodeToBoundNodeEdgesField);
           // queryEdgeCount++;
           // LOG.info("{} processingQuery bolt received PN to BN edges",this.taskID);
            if(!queryNodeToBNEdgeMap.isEmpty())
            {
                this.schgra.addQNToBNEdges(queryNodeToBNEdgeMap);
            }
            else
            {
                LOG.info("{} processingQuery bolt received an empty edges from PN to BN", this.taskID);
            }
            //this.schgra.addQNToBNEdges(queryNodeToBNEdgeMap);
        }

        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ModifiedQueryEdgesSendFinish))  //add the edges from query node to boundary nodes into the skeleton graph
        {
            //long timeSendFromSubgraphBolt=(long) input.getValueByField(VirtualHopStreamFields.ModifiedQueryEdgesSendFinishFieldTime);

            queryEdgeSubgraphBoltCount++;
            if(queryEdgeSubgraphBoltCount==VirtualHopsBoltNumberConfig.VirtualHopsSubgraphBoltNumber)
            {
               // long queryOnQPBoltStartTime=System.currentTimeMillis();
                collector.emit(VirtualHopStreamIDs.ModifiedAddQNodeToBNodeEdgesFinished, new Values(true));
                //LOG.info("{} processing query bolt finish adding the edges from query node to boundary nodes", this.taskID);
            }
        }

        if (input.getSourceStreamId().equals(VirtualHopStreamIDs.KSPQueryToBeProcessed)) {
            Query query = (Query) input.getValueByField(VirtualHopStreamFields.KSPQuery);

            long startProcessingTime=System.currentTimeMillis();
           // LOG.info("{} receives {}th query!",taskID,query.queryID);
            if (query.queryID == 0) {
                long startTime = System.currentTimeMillis();
            }
            //System.out.println("Received "+query.queryID+"th query!");
            this.partialPathcountMap.put(query.queryID, 0);   //to record the number of subgraphBolts have returned the partial results

            List<Path> hypoPathList = null;
            try {
                hypoPathList = computeLowerBoundPaths(query, this.schgra, boltID);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (hypoPathList.size() == 0)
            {
                System.out.println("No hypo paths for "+query.queryID+" query");
            }
            Collections.sort(hypoPathList);
            //---------------------2019.101.13  new code
            if(this.HypoPathsForQueryMap.containsKey(query.queryID))
            {
                this.HypoPathsForQueryMap.remove(query.queryID);
            }
            //---------------------2019.101.13  new code
            this.HypoPathsForQueryMap.put(query.queryID, hypoPathList);

            //System.out.println("Lower bound paths have been found!");
            this.HypoPathsForQueryMap.get(query.queryID).get(0).hypoFlag = true;     //emit one hypoPath of queryID to subgraphBolts
            //long currentTime=System.currentTimeMillis();
            collector.emit(VirtualHopStreamIDs.OneHypotheticalPath, new Values(this.taskID, false, query.queryID, this.HypoPathsForQueryMap.get(query.queryID).get(0)));
            long endProcessingTime=System.currentTimeMillis();

            //Summary the processing time of one query
            queryProcessingTimeMap.put(query.queryID,endProcessingTime-startProcessingTime);
        }

        if (input.getSourceStreamId().equals(VirtualHopStreamIDs.KPartialShortestPaths))
        {
            int queryID = (int) input.getValueByField(VirtualHopStreamFields.QueryIDReturnedBySubgraphBolt);
            long usedTimeForHypoPath=(long)input.getValueByField(VirtualHopStreamFields.timeForHypoPathSubGrphBolt);
            //String labelCount=(String) input.getValueByField(VirtualHopStreamFields.labelCount);
            int partialKSPcount = this.partialPathcountMap.get(queryID);
            partialKSPcount++;

            this.partialPathcountMap.put(queryID, partialKSPcount);

            Map<Points, List<Path>> currentPartialPathsMap = (Map<Points, List<Path>>) input.getValueByField(VirtualHopStreamFields.PartitialKShortestPaths);//partial paths emitted by subgraphBolts

            if(this.partialKSPQueryMap.keySet().contains(queryID))
            {
                Map<Points, List<Path>> existPartialPathMap=this.partialKSPQueryMap.get(queryID);
                if(currentPartialPathsMap.size()!=0) {
                    for (Points curPoint : currentPartialPathsMap.keySet()) {
                        List<Path> curPartialPathsList = currentPartialPathsMap.get(curPoint);
                        if (existPartialPathMap.keySet().contains(curPoint)) {
                            List<Path> existPartialPathsList = existPartialPathMap.get(curPoint);
                            for (Path curPartialPath : curPartialPathsList) {
                                if (!existPartialPathsList.contains(curPartialPath)) {
                                    existPartialPathsList.add(curPartialPath);
                                }
                            }
                        } else {
                            existPartialPathMap.put(curPoint, curPartialPathsList);
                        }
                    }
                    this.partialKSPQueryMap.put(queryID, existPartialPathMap);
                }
            }
            else
            {
                if(currentPartialPathsMap.size()!=0)
                {
                    this.partialKSPQueryMap.put(queryID,currentPartialPathsMap);
                }
            }
            if (partialKSPcount == VirtualHopsBoltNumberConfig.VirtualHopsSubgraphBoltNumber)    //if all subgraphBolts return the partial paths
            {
                long existingUsedTime=queryProcessingTimeMap.get(queryID);
                existingUsedTime=existingUsedTime+usedTimeForHypoPath;
                queryProcessingTimeMap.put(queryID,existingUsedTime);

                long partialStartTime=System.currentTimeMillis();
                Path currentHypoPath = null;
                Path nextHypoPath = null;
                int boundOfHypoPathNumber = 0;
                List<Path> hypoPathList=this.HypoPathsForQueryMap.get(queryID);
                //Collections.sort(hypoPathList);
               // System.out.println("hypoPathList size: "+hypoPathList.size());
                for (int p = 0; p < hypoPathList.size(); p++) {
                  //  System.out.println("hypoPathList.get(p).hypoFlag: "+hypoPathList.get(p).hypoFlag);
                    if (hypoPathList.get(p).hypoFlag == false) {
                        nextHypoPath = hypoPathList.get(p);
                        currentHypoPath = hypoPathList.get(p - 1);
                        boundOfHypoPathNumber = p;
                        break;
                    }
                }

                if (currentHypoPath == null) {
                    //System.out.println("Cannot determine KSP for "+queryID+"th query!");
                    LOG.info("Cannot determine KSP for {}th query!", queryID);
                }

                List<Path> realFinalPaths = getRealKPaths(currentHypoPath, this.partialKSPQueryMap.get(queryID));   //compute the real k paths based on the hypoPath
                //this.partialKSPQueryMap.get(queryID).clear();
                List<Path> kspList;
                if(this.KSPQueryMap.containsKey(queryID))
                {
                    kspList=KSPQueryMap.get(queryID);
                }
                else
                {
                    kspList=new ArrayList<Path>();
                }
                for (Path currentPath : realFinalPaths) {
                    if (!kspList.contains(currentPath)) {
                        kspList.add(currentPath);
                    }
                }
                Collections.sort(kspList);

                if (kspList.size() > VirtualHopsParameter.queryK) {
                    while (kspList.size() > VirtualHopsParameter.queryK) {
                        kspList.remove(kspList.size() - 1);
                    }
                }
                KSPQueryMap.put(queryID,kspList);
                //System.out.println("Queue size: "+this.backwardRealKSPQueue.size());

                long partialEndTime=System.currentTimeMillis();
                long existingMapTime=queryProcessingTimeMap.get(queryID);
                existingMapTime=existingMapTime+(partialEndTime-partialStartTime);
                queryProcessingTimeMap.put(queryID,existingMapTime);
                Path path = kspList.get(kspList.size() - 1);              //get the current kth path
                if(boundOfHypoPathNumber==VirtualHopsParameter.queryK*2-1)
                {
                    System.out.println();
                    //System.out.println("KSP for " + queryID + "th query have been found!!!!");
                    LOG.info("KSP for {}th query have been found!!!!",queryID);
                    LOG.info("{}: result of {}th query has been emitted!", taskID,queryID);
                    collector.emit(VirtualHopStreamIDs.QueryKSPResult, new Values(kspList,boundOfHypoPathNumber,queryProcessingTimeMap.get(queryID))); //send the query result to queryGenerateBolt

                    //Nextline is new code
                    this.KSPQueryMap.remove(queryID);
                    //------------------2019.10.14
                    this.partialKSPQueryMap.remove(queryID);
                    this.HypoPathsForQueryMap.remove(queryID);
                }
                else {
                    if (path.length < nextHypoPath.length || path.length == nextHypoPath.length) {

                        //LOG.info("QueryID: {}; Round finished!!!",queryID);
                        //LOG.info("Number of iterations: {}", boundOfHypoPathNumber);
                        //LOG.info("kth path length: {}", path.length);
                        //LOG.info("nextHypoPath length: {}", nextHypoPath.length);
                        //LOG.info("currentHypoPath length: {}", currentHypoPath.length);
                        collector.emit(VirtualHopStreamIDs.QueryKSPResult, new Values(kspList,boundOfHypoPathNumber,queryProcessingTimeMap.get(queryID))); //send the query result to queryGenerateBolt

                        this.KSPQueryMap.remove(queryID);
                        this.partialKSPQueryMap.remove(queryID);
                        this.HypoPathsForQueryMap.remove(queryID);

                    } else {
                        nextHypoPath.hypoFlag = true;

                        //-----------------------------------------new code
                        //this set contains the points in nextHypoPath but not currentHypoPath
                        Set<Points> filteredPointSet=filterOverlapPath(currentHypoPath,nextHypoPath);
//                        if(queryID==2)
//                        {
//                            LOG.info("QueryId:{}, the length of nextHypoPath is {}",queryID,nextHypoPath);
//                            LOG.info("QueryId:{}, the number of non-overlap points is {}",queryID,filteredPointSet.size());
//                        }
                        Set<Points> nextPathPointsSet=new HashSet<>();
                        for(int h=0;h<nextHypoPath.line.size()-1;h++)
                        {
                            Points point=new Points(nextHypoPath.line.get(h).id,nextHypoPath.line.get(h+1).id);
                            nextPathPointsSet.add(point);
                        }
                        for(Points point:filteredPointSet)
                        {
                            nextPathPointsSet.remove(point);
                        }
                        //------------------2019.10.14
                        Set<Points> tempoPointSet=new ConcurrentHashSet<>();
                        tempoPointSet.addAll(this.partialKSPQueryMap.get(queryID).keySet());
                        for(Points point:tempoPointSet)
                        {
                            if(nextPathPointsSet.contains(point))
                            {
                                continue;
                            }
                            else
                            {
                                this.partialKSPQueryMap.get(queryID).remove(point);
                            }
                        }
                        this.partialPathcountMap.put(queryID, 0);
                        collector.emit(VirtualHopStreamIDs.OneHypotheticalPath, new Values(this.taskID, true, queryID, filteredPointSet));

                        //-----------------------------------------------
                        //System.out.println(boltID + ": the " + boundOfHypoPathNumber + "th hypo path has been emit!");

                        //------------------------------------------------ original code
//                        this.partialKSPQueryMap.get(queryID).clear();
//                        this.partialPathcountMap.put(queryID, 0);
//                        collector.emit(VirtualHopStreamIDs.OneHypotheticalPath, new Values(this.taskID, false, queryID, nextHypoPath));
                        //---------------------------------------------------------
                    }
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(VirtualHopStreamIDs.OneHypotheticalPath,
                new Fields(VirtualHopStreamFields.QueryProcessingBoltTaskID, VirtualHopStreamFields.FilteredPointsFlag,VirtualHopStreamFields.QueryIDLableHypoPath, VirtualHopStreamFields.HypotheticalPath));

        declarer.declareStream(VirtualHopStreamIDs.QueryKSPResult,
                new Fields(VirtualHopStreamFields.KSPResult, VirtualHopStreamFields.roundNumber,VirtualHopStreamFields.totalProcessingTimeQPBolt) );

        declarer.declareStream(VirtualHopStreamIDs.SkeletonGraphBuilt,
                new Fields(VirtualHopStreamFields.SkeletonGraphBuiltField));

        declarer.declareStream(VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish,
                new Fields(VirtualHopStreamFields.QueProBoltUpdateSkeGraFinishField));
        declarer.declareStream(VirtualHopStreamIDs.ModifiedAddQNodeToBNodeEdgesFinished,
                new Fields(VirtualHopStreamFields.ModifiedAddQNodeToBNodeEdgesFinishedField));
    }

    public List<Path> computeLowerBoundPaths(Query query, SchemaGraph copySchemaGraph, String boltID) throws InterruptedException {
        List<Path> hypoPathList = new ArrayList<Path>();
        KSPYenparallelGraph krg = new KSPYenparallelGraph();
        //System.out.println("Query first node id: "+query.sourceNodeID+"; query second node id: "+query.endNodeID+"; k is "+query.k);

        List<List<Node>> kHypoPaths = krg.kspYenWithoutLoops(boltID,
                query.k*2,
                query.sourceNodeID,
                query.endNodeID,
                copySchemaGraph.schemaGraph,
                copySchemaGraph.minimumSchemaEdgeMap,
                copySchemaGraph.yenFlagMap);

        for (List<Node> hypoPath : kHypoPaths) {
            double length = 0.0;
            for (int i = 0; i < hypoPath.size() - 1; i++) {
                Node firstNode = hypoPath.get(i);
                Node secondNode = hypoPath.get(i + 1);
                Points point = new Points(firstNode.id, secondNode.id);
                length = length + copySchemaGraph.minimumSchemaEdgeMap.get(point);
            }
            Path path = new Path(hypoPath, length);
            hypoPathList.add(path);
        }
        return hypoPathList;
    }
    public List<Path> getRealKPaths(Path hypotheticalPath, Map<Points, List<Path>> allPartialPathsMap)    //For a given hypothetical path, compute k shortest paths based on the subgraph paths segments in the map
    {
        List<Path> realPathsList=new ArrayList<Path>();
        for(int j=0;j<(hypotheticalPath.line.size()-1);j++)
        {
            Node startNode=hypotheticalPath.line.get(j);
            Node secondNode=hypotheticalPath.line.get(j+1);
            Points point=new Points(startNode.id,secondNode.id);
            List<Path> subgraphPathList=allPartialPathsMap.get(point);
            //System.out.println("The first node:"+startNode.id+", second node: "+secondNode.id);

            if(subgraphPathList==null)
            {
             //   System.out.println("The size of allPartialPathsMap:"+allPartialPathsMap.size());
                System.out.println("There is no subgraph paths between "+startNode.id+" and "+secondNode.id);
                LOG.info("There is no subgraph paths between {} and {}", startNode.id, secondNode.id);
            }
            if(realPathsList.isEmpty())
            {
                realPathsList.addAll(subgraphPathList);
                Collections.sort(realPathsList);
                //  System.out.println("The first node:"+startNode.id+", second node: "+secondNode.id);
            }
            else
            {
                List<Path> joinRealPathsList=new ArrayList<Path>();
                for(Path prePath:realPathsList)
                {
                    for(Path curPath:subgraphPathList)
                    {
                        List<Node> mergeLine=new ArrayList<>();
                        mergeLine.addAll(prePath.line);
                        mergeLine.remove(mergeLine.size()-1);             //delete the same boundary node
                        mergeLine.addAll(curPath.line);
                        double mergeLength=prePath.length+curPath.length;
                        Path mergePath=new Path(mergeLine,mergeLength);
                        joinRealPathsList.add(mergePath);
                    }
                }
                realPathsList.clear();
                if(joinRealPathsList.size()<VirtualHopsParameter.queryK)
                {
                    realPathsList.addAll(joinRealPathsList);
                }
                else
                {
                    Collections.sort(joinRealPathsList);
                    for(int join=0;join<VirtualHopsParameter.queryK;join++)
                    {
                        realPathsList.add(joinRealPathsList.get(join));
                    }
                }
            }
        }
        // System.out.println("realPathsList size: "+realPathsList.size());
        return realPathsList;
    }
    public Set<Points> filterOverlapPath(Path curPath, Path nextPath)
    {
       // Path differentPath=null;
        Set<Points> curLinePointsSet=new HashSet<>();
        Set<Points> nextLinePointsSet=new HashSet<>();
        for(int i=0;i<curPath.line.size()-1;i++)
        {
           Points point=new Points(curPath.line.get(i).id,curPath.line.get(i+1).id);
            curLinePointsSet.add(point);
        }
        for(int j=0;j<nextPath.line.size()-1;j++)
        {
            Points point=new Points(nextPath.line.get(j).id,nextPath.line.get(j+1).id);
            nextLinePointsSet.add(point);
        }
        for(Points point:curLinePointsSet)
        {
            if(nextLinePointsSet.contains(point))
            {
                nextLinePointsSet.remove(point);
            }
        }

        return nextLinePointsSet;
    }
//    public void writeRateGraph(long Time)
//    {
//        String writeAddress="/root/testData/result.txt";
//        System.out.println("Writing starts:");
//        try {
//            File file = new File(writeAddress);
//            if(file.exists()){
//                FileWriter fw = new FileWriter(file,true);
//                BufferedWriter bw = new BufferedWriter(fw);
//                String line="Current, time of build skeleton graph is "+Time;
//                bw.write(line);
//                bw.write("\n");
//                System.out.println("Writing is finished!");
//                bw.close();
//                fw.close();
//
//            }
//        } catch (Exception e) {
//            // TODO: handle exception
//        }
//    }

}
