package KSPOnStorm.VirtualHopsBolt;

import KSPOnStorm.*;
import org.apache.storm.topology.base.BaseRichBolt;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.awt.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.List;

public class VirtualHopsConstructGraphBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsConstructGraphBolt.class);
    OutputCollector collector;

    public HashMap<Node, List<Node>> graph=new HashMap<Node, List<Node>>();
    public HashMap<Points,Double> edgeMap=new HashMap <Points,Double>();
    public List<Partition> partitionList=new ArrayList<Partition>();
    public String subGraphAssignFlag="PF";
    int partitionCount=0;
    int graphLineCount=0;
    long totalMemory=0;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdateGraphTriggerFlag)) //if receiving a updateGraphFlag, send the update edges weights to subgraphs
        {
            updateGraph();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(VirtualHopStreamIDs.UpdateGraphSendFinishFlag, new Values(true));
        }

//        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.UpdateGraphSendFlag)) //if receiving a updateGraphFlag, send the update edges weights to subgraphs
//        {
//            updateGraph();
//        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.fininshReadGraphFlag))//if the graph read is finished, partition the graph
        {
            LOG.info("ConstructGraphBolt receives {} lines!!!", graphLineCount);
            LOG.info("The graph size:{}", graph.keySet().size());
            //System.out.println("The graph size: "+graph.keySet().size());

            PartitionGraphUniformly pg=new PartitionGraphUniformly();
            this.partitionList=pg.divide(this.graph);
            //System.out.println("Number of subgraphs: "+partitionList.size());
            LOG.info("Number of subgraphs: {}", partitionList.size());
            int averageSubgraphSize=0;
            int averageBoundaryNodeSize=0;
            for(Partition par:partitionList)
            {
                averageSubgraphSize=averageSubgraphSize+par.subGraph.keySet().size();
                averageBoundaryNodeSize=averageBoundaryNodeSize+par.boundaryNodeList.size();
            }
            averageSubgraphSize=averageSubgraphSize/partitionList.size();
            averageBoundaryNodeSize=averageBoundaryNodeSize/partitionList.size();
            LOG.info("Average size of every subgraph: {}", averageSubgraphSize);
            LOG.info("Average boundary nodes of every subgraph: {}", averageBoundaryNodeSize);
//            try {
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            for(Partition par:this.partitionList)
//            {
//                if(par.boundaryNodeList.size()>1)
//                {
//                    partitionCount++;
//                }
//            }
            //  System.out.println("Total number of subgraphs:"+partitionCount);
            long partitionTime=System.currentTimeMillis();

        //    List<Partition> parList=new ArrayList<>();
            while(partitionList.isEmpty()==false)
            {
                //parList.add(par);
                Partition cpPar=partitionList.remove(0);
                cpPar.subEdgeMap=computeSubEdgeMap(cpPar);
                collector.emit(VirtualHopStreamIDs.SUBGRAPH_MAP,new Values(cpPar.partitionId, cpPar));    //emit each subgraph to the SchemaGraphBolt
                try {
                    Thread.sleep(10);
                    //new code
                   // par.subGraph.clear();    //clear the subgraph
                   // par.subEdgeMap.clear();  //clear the edgeset
//                    par.boundaryNodeList.clear();   //new code to reduce memory
//                    par.subGraph.clear();
//                    par.subEdgeMap.clear();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            collector.emit(VirtualHopStreamIDs.subGraphAssignFlag,new Values(subGraphAssignFlag,partitionTime));     ////emit a flag that means all generated subgraphs haven been sent to different SubgraphBolts
            this.partitionList.clear();  //new code to reduce memory
//            this.totalMemory=Runtime.getRuntime().totalMemory();
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            LOG.info("ConstructGraphBolt: The initial used memory: {}", totalMemory);
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.SPOUT_ReadGaph))   //if it receives a line in the graph, and processes this line. Then build a complete graph
        {
            if(graphLineCount==0)
            {
                LOG.info("ConstructGraphBolt: begin to receive the graph line" );
            }
            graphLineCount++;
            Node startNode=(Node)input.getValueByField("SN");
            Node endNode=(Node)input.getValueByField("EN");
            double weight=input.getDoubleByField("EW");
            processLine(startNode,endNode,weight);
        }
    }
    public HashMap<Points,Double> computeSubEdgeMap(Partition par)
    {
        Iterator<Node> iterator=par.subGraph.keySet().iterator();
        HashMap<Points,Double> subEdgeMap=new HashMap<Points,Double>();
        while (iterator.hasNext())
        {
            Node keyNode=(Node)iterator.next();
            //System.out.print(keyNode.id+": ");
            for(int i=0;i<par.subGraph.get(keyNode).size();i++)
            {
                //System.out.print(subGraph.get(keyNode).get(i).id+", ");
                Points point=new Points(keyNode.id, par.subGraph.get(keyNode).get(i).id);
                subEdgeMap.put(point, this.edgeMap.get(point));
            }
            //System.out.println();
        }
        return subEdgeMap;
    }
    public void processLine(Node snd, Node tnd, double edgeWeight)
    {
        if(!graph.keySet().contains(snd))
        {
            //	System.out.println(graph.keySet().contains(snd));
            graph.put(snd, new ArrayList<Node>());
            graph.get(snd).add(tnd);
            edgeMap.put(new Points(snd.id,tnd.id), edgeWeight);
        }
        else
        {
            graph.get(snd).add(tnd);
            edgeMap.put(new Points(snd.id,tnd.id), edgeWeight);
        }
    }

public void updateGraph() {
    //Points[] pointsArray = this.edgeMap.keySet().toArray(new Points[0]);
    Set<Points> origialPointsSet = new HashSet<>();
    //origialPointsList.addAll(this.edgeMap.keySet());
    LOG.info("UpdateGraph begins");
    for (Points point:this.edgeMap.keySet())
    {
        Points rePoint=new Points(point.tnd,point.snd);
        if(origialPointsSet.contains(rePoint)||origialPointsSet.contains(point))
        {
            continue;
        }
        else
        {
            origialPointsSet.add(point);
        }
    }
    LOG.info("origialPointsSet size: "+origialPointsSet.size());
    List<Points> origialPointsList=new ArrayList<>();
    origialPointsList.addAll(origialPointsSet);
    Random rand=new Random();
    //List<Points> updatedPointsList=new ArrayList<>();
    Set<Points> updatePointSet=new HashSet<>();
    // Points point=pointsArray[rand.nextInt(pointsArray.length)];
    int updatedNumber=(new Double(origialPointsList.size()*0.5)).intValue();
    LOG.info("ConstructBolt: Number of points to be updated: "+updatedNumber);
    while(updatePointSet.size()<updatedNumber)
    {
        //iter++;
        int location=rand.nextInt(origialPointsList.size());
        Points point=origialPointsList.get(location);

        updatePointSet.add(point);
        origialPointsList.remove(location);
    }
    LOG.info("Number of edges to be update is {}", updatePointSet.size());
    for(Points point: updatePointSet)
    {
        collector.emit(VirtualHopStreamIDs.UpdateEdgePoints, new Values(point));
    }
    LOG.info("Update is finished");
}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(VirtualHopStreamIDs.SUBGRAPH_MAP,                       //declare emit a subgraph to a subgraphBolt
                new Fields(VirtualHopStreamFields.Subgraph_id,VirtualHopStreamFields.Subgraph));

        declarer.declareStream(VirtualHopStreamIDs.subGraphAssignFlag,                 //declare all subgraphs have been emitted
                new Fields(VirtualHopStreamFields.SubGraphAssign_Flag,VirtualHopStreamFields.startTime));

        declarer.declareStream(VirtualHopStreamIDs.UpdateEdgePoints,
                new Fields(VirtualHopStreamFields.UpdateEdgePoint));

        declarer.declareStream(VirtualHopStreamIDs.UpdateGraphSendFinishFlag,
                new Fields(VirtualHopStreamFields.UpdateGraphSendFinishFlagField));
    }
}
