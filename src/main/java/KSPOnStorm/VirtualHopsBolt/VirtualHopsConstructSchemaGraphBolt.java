package KSPOnStorm.VirtualHopsBolt;

import KSPOnStorm.*;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import KSPOnStorm.VirtualHopsData.VirtualHopsBoltNumberConfig;
import KSPOnStorm.VirtualHopsData.VirtualHopsParameter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

public class VirtualHopsConstructSchemaGraphBolt extends BaseRichBolt {// 19.6.10

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsConstructSchemaGraphBolt.class);

    OutputCollector collector;
    List<SubGraph> subGraphList=new LinkedList<SubGraph>();
    //Queue<Path> backwardRealKSPQueue=new PriorityQueue<Path>();
    //List<Path> finalKSPList=new ArrayList<Path>();
    //List<Path> HypotheticalPaths=new LinkedList<Path>();
    //Map<Points, List<Path>> allPartialPathsMap=new HashMap<Points, List<Path>>();
    SchemaGraph schgra;

    int subgraphBoltCount=0;
    int partialKSPcount=0;
    int receivedSGEdgeMapCount=0;
    int receievedSkeletonGraphCount=0;

    long startTime=0;
    long endTime=0;
    @Override
    public  void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        schgra =new SchemaGraph();

        //   allPartialPathsMap
    }
    @Override
    public void execute(Tuple input) {
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.boundPathSubgraph))   //receive a subgraph including lowerbound paths between any pair of boundary nodes
        {
            SubGraph sg=(SubGraph) input.getValueByField(VirtualHopStreamFields.LowerBoundPathSubgraph);
            subGraphList.add(sg);
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
                //this.schgra.buildSchemaGraph(this.subGraphList);               //construct a schema graph
                System.out.println("The schema graph is built!");
                this.subGraphList.clear();
                LOG.info("Number of points: {}", this.schgra.minimumSchemaEdgeMap.keySet().size());
                endTime=System.currentTimeMillis();
                long buildingTime=endTime-this.startTime;
                System.out.println("Time of building graph: "+(buildingTime));
                LOG.info("Time of building skeleton graph: {}", buildingTime);
                writeRateGraph(buildingTime);
                //System.out.println("The schema graph size: "+this.schgra.minimumSchemaEdgeMap.keySet().size());
                //WriteSchemaGraphToText wst=new WriteSchemaGraphToText();
                //wst.writeSchemaGraph(this.schgra.minimumSchemaEdgeMap);
                //System.out.println("Writing graph is built!");
                this.schgra.schemaEdgeMap.clear();
                //collector.emit(VirtualHopStreamIDs.SkeletonGraph,new Values(this.schgra.schemaGraph)); //send skeleton graph to processQueryBolt
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //collector.emit(VirtualHopStreamIDs.SkeletonGraphEdgeMap, new Values(this.schgra.minimumSchemaEdgeMap));

                //collector.emit(VirtualHopStreamIDs.SchamaGraphEmit,new Values(schgra));

               // collector.emit(VirtualHopStreamIDs.QuerySend_Flag, new Values(true));  //send a flag to the queryGeneratebolt, and ask queryGeneratebolt to send a query
             //   System.out.println("VirtualHopsConstructSchemaGraphBolt: Send Query Flag");
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ReceivedSkeletonGraph))
        {
            receievedSkeletonGraphCount++;
            if(receievedSkeletonGraphCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber) //if all processQueryBolt receives skeleton graph
            {
                collector.emit(VirtualHopStreamIDs.SkeletonGraphEdgeMap, new Values(this.schgra.minimumSchemaEdgeMap)); //send the edgeMap of skeleton graph to processQueryBolt
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ReceivedSkeletonGraphEdgeMap))
        {
            this.receivedSGEdgeMapCount++;
            if(this.receivedSGEdgeMapCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber) //if all processQueryBolt receive the edgeMap of skeleton graph
            {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                collector.emit(VirtualHopStreamIDs.QuerySend_Flag,new Values(true));                     //told queryGenerateBolt send query.
            }
        }
    }

    public void writeRateGraph(long Time)
    {
        //String writeAddress="C:\\Users\\esouser\\Desktop\\Graph\\result.txt";
        String writeAddress="/root/testData/result.txt";
        System.out.println("Writing starts:");
        try {
            File file = new File(writeAddress);
            if(file.exists()){
                FileWriter fw = new FileWriter(file,false);
                BufferedWriter bw = new BufferedWriter(fw);
                String line="Current, time of build skeleton graph is "+Time;
                bw.write(line);
                bw.write("\n");
                System.out.println("Writing is finished!");
                bw.close();
                fw.close();

            }
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
    public HashMap<Node, List<Node>> duplicateGraph (HashMap<Node, List<Node>> graph)//, TimeThread tt)
    {
        HashMap<Node, List<Node>> copyGraph=new HashMap<Node, List<Node>>();
        Iterator<Node> iterator=graph.keySet().iterator();
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
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*declarer.declareStream(VirtualHopStreamIDs.QuerySend_Flag,
                new Fields(VirtualHopStreamFields.triggerQuerySend_Flag));
        declarer.declareStream(VirtualHopStreamIDs.OneHypotheticalPath,
                new Fields(VirtualHopStreamFields.HypotheticalPath));*/
//        declarer.declareStream(VirtualHopStreamIDs.SkeletonGraph,
//                new Fields(VirtualHopStreamFields.SkeletonGraph));
        declarer.declareStream(VirtualHopStreamIDs.SkeletonGraphEdgeMap,
                new Fields(VirtualHopStreamFields.SkeletonGraphEdgeMap));
        //declarer.declareStream(VirtualHopStreamIDs.SchamaGraphEmit,
        //        new Fields(VirtualHopStreamFields.SchemaGraphInforamtion));
        declarer.declareStream(VirtualHopStreamIDs.QuerySend_Flag,
                new Fields(VirtualHopStreamFields.triggerQuerySend_Flag));}
}
