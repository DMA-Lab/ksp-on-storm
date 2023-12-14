package KSPOnStorm.VirturalHopsTopology;

import KSPOnStorm.VirtualHopsSpout.VirtualHopsGraphReaderSpout;
import KSPOnStorm.VirtualHopsBolt.*;
import KSPOnStorm.VirtualHopsData.*;
import KSPOnStorm.util.Configuration;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import java.io.IOException;
//this is new version
public class VirtualHopsKSPOnStormTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException, IOException, ClassNotFoundException {
        TopologyBuilder builder = new TopologyBuilder();

//        Config conf =
//                args.length > 0 ? Configuration.getConfiguration("knnbr", args[0]).createStormConfig()
//                        : Configuration.getConfiguration("knnbr").createStormConfig();

        Config conf = Configuration.getConfiguration("knnbr").createStormConfig();

        //VirtualHopsParameter.partitionScale=Integer.parseInt(args[0]);

        builder.setSpout("VirtualHopsGraphReaderSpout", new VirtualHopsGraphReaderSpout(
                String.valueOf(conf.get("params.graph.path"))),1);

        builder.setBolt("VirtualHopsConstructGraphBolt", new VirtualHopsConstructGraphBolt(), 1)
                // collect the lines of the graph
                .globalGrouping("VirtualHopsGraphReaderSpout", VirtualHopStreamIDs.SPOUT_ReadGaph)
                //receive a flag to send updated graph to sub graph bolts
                .globalGrouping("VirtualHopsModifiedQueryGenerateBolt",VirtualHopStreamIDs.UpdateGraphTriggerFlag)
                // monitor the flag that means the reading graph is finished
                .globalGrouping("VirtualHopsGraphReaderSpout", VirtualHopStreamIDs.fininshReadGraphFlag);


        builder.setBolt("VirtualHopsSubgraphBolt", new VirtualHopsSubgraphBolt(), VirtualHopsBoltNumberConfig.VirtualHopsSubgraphBoltNumber)//.setNumTasks(6)
                //receives a query that needs to be added skeleton graph first
                .allGrouping("VirtualHopsModifiedQueryGenerateBolt",VirtualHopStreamIDs.ModifiedQueryToSubgraphBolt)
                // assign subgraphs to different SubGraphBolts randomly
                .shuffleGrouping("VirtualHopsConstructGraphBolt", VirtualHopStreamIDs.SUBGRAPH_MAP)
                // the flag meaning the assignment of subGraphs is finished
                .allGrouping("VirtualHopsConstructGraphBolt", VirtualHopStreamIDs.subGraphAssignFlag)
                // receive a hypothetical path
                .allGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.OneHypotheticalPath)
                // receive the points with varying weights
                .allGrouping("VirtualHopsConstructGraphBolt", VirtualHopStreamIDs.UpdateEdgePoints)
                // receive a flag that means sending the points with varying weights is finished
                .allGrouping("VirtualHopsConstructGraphBolt",VirtualHopStreamIDs.UpdateGraphSendFinishFlag);

//        builder.setBolt("VirtualHopsConstructSchemaGraphBolt", new VirtualHopsConstructSchemaGraphBolt(), 1)
//                //receive every SubGraph
//                .globalGrouping("VirtualHopsSubgraphBolt", VirtualHopStreamIDs.boundPathSubgraph)
//                //receive a flag meaning subGraph emitting is finished
//                .globalGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.subGraphSendFlag)
//                //collect ReceivedSkeletonGraph from VirtualHopsQueryProcessingBolts to guarantee they all receive the schemagraph
//                .globalGrouping("VirtualHopsQueryProcessingBolt", VirtualHopStreamIDs.ReceivedSkeletonGraph)
//                //collect receivedSchemaGraphFlags from VirtualHopsQueryProcessingBolts to guarantee they all receive the schemagraph edgeMap, then it informs the virtualHopsQueryGenerateBolt to generate query.
//                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.ReceivedSkeletonGraphEdgeMap);


        builder.setBolt("VirtualHopsQueryProcessingBolt", new VirtualHopsQueryProcessingBolt(),VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.ModifiedQueryEdgesSendFinish)
                //receive the edges between the query node to the boundary nodes in its subgraph
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.ModifiedQueryNodeToBoundNodeEdges)
                //receive the subgraphs emitted from VirtualHopsConstructSchemaGraphBolt
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.boundPathSubgraph)
                //receive the flag that means a subgraphBolt finishes emitting the subgraphs
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.subGraphSendFlag)
                //receive the updated hypothetical lower bound paths of every subgraph from subgraph bolts.
                .allGrouping("VirtualHopsSubgraphBolt", VirtualHopStreamIDs.UpdatedLowerBoundPaths)
                //receive a flag that means the transmission of updated hypothetical lower bound paths of all subgraphs is finished
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.SubgraphSendUpdatedLBPathFinish)
                //receive queries to be processed that are assigned randomly
                .shuffleGrouping("VirtualHopsModifiedQueryGenerateBolt", VirtualHopStreamIDs.KSPQueryToBeProcessed)
                //receive the partial KSP from subgraphBolt
                .directGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.KPartialShortestPaths)
                //receive the start time of update edge
                .allGrouping("VirtualHopsSubgraphBolt",VirtualHopStreamIDs.UpdateSubgraphStartTime);

//        builder.setBolt("VirtualHopsQueryGenerateBolt", new VirtualHopsQueryGenerateBolt(), 1)
//                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.ModifiedAddQNodeToBNodeEdgesFinished)
//                //receive a flag that requires to send queries
//                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.SkeletonGraphBuilt)
//                // receive a flag that requires to send queries again
//                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish)
//                // receive the query results from every query process bolt
//                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.QueryKSPResult);

        builder.setBolt("VirtualHopsModifiedQueryGenerateBolt", new VirtualHopsModifiedQueryGenerateBolt(), 1)
                //receive a flag that means the query node being not a boundary node has been added into skeleton graph
                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.ModifiedAddQNodeToBNodeEdgesFinished)
                //receive a flag that requires to send queries
                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.SkeletonGraphBuilt)
                // receive a flag that requires to send queries again
                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish)
                // receive the query results from every query process bolt
                .globalGrouping("VirtualHopsQueryProcessingBolt",VirtualHopStreamIDs.QueryKSPResult);


        if (Boolean.valueOf(conf.get("local").toString())) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("VirtualHopsKSPOnStormTopology", conf, builder.createTopology());
            //Thread.sleep(1000);
        } else {
            conf.setNumWorkers(10);
            //conf.setMaxTaskParallelism(1);
            StormSubmitter.submitTopologyWithProgressBar("VirtualHopsKSPOnStormTopology", conf, builder.createTopology());
        }
    }
}
