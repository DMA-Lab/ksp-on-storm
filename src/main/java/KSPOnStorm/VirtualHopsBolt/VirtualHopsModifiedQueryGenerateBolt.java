package KSPOnStorm.VirtualHopsBolt;

import KSPOnStorm.Path;
import KSPOnStorm.Query;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
//this is a new version 2019.6.10
public class VirtualHopsModifiedQueryGenerateBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsModifiedQueryGenerateBolt.class);

    OutputCollector collector;
    List<Query> generatedQueryList;
    String boltID=null;
    long queryStartTime=0;
    int resultCount=0;
    //    Map<Integer, Integer> roundQueryCountMap=null;
    int round=0;
    int skeletonGraphBuiltCount=0;
    int skeletonGraphUpdateCount=0;
    int skeletonGraphAddQEdgeCount=0;
    int QueryRound=0;
    int totalRoundNumber=0;
    long totalMemory=0;
    long totalProcessingTime=0;
    boolean updateFlag=false;
    boolean iterFlag=false;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        generatedQueryList=new ArrayList<Query>();
        boltID=context.getThisComponentId()+context.getThisTaskId();
        //roundQueryCountMap=new ConcurrentHashMap<>();
    }
    @Override
    public void execute(Tuple input)
    {
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.SkeletonGraphBuilt))  //send this query to subGraphBolts to replenish the skeleton graph
        {
            this.skeletonGraphAddQEdgeCount++;
            long serverMemory=(long)input.getValueByField(VirtualHopStreamFields.SkeletonGraphBuiltField);
            totalMemory=totalMemory+serverMemory;
            LOG.info("{} processing Query Bolt is finished", this.skeletonGraphAddQEdgeCount);
            if(this.skeletonGraphAddQEdgeCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
            {
                LOG.info("Modified queryBolt: new query is sent to subgraph bolts");
                LOG.info("Total memory of all servers is {}",totalMemory);
                Query query=new Query(1,5000 ,0);
                //Query query=new Query(5000,15000,0);
                //Query query=new Query(12000,15000,0);
                //Query query=new Query(5000,15000,0);
                //Query query=new Query(5000,15000,0);
                //The next line is to trigger sending query
                collector.emit(VirtualHopStreamIDs.ModifiedQueryToSubgraphBolt,new Values(query));
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish))
        {
            this.skeletonGraphUpdateCount++;
            if(this.skeletonGraphUpdateCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
            {
                this.QueryRound++;
                LOG.info("Update query start time: {}", queryStartTime);
                //this.updateFlag=true;
                this.iterFlag=true;
                // System.out.println("Second round query start time: "+queryStartTime);
                round=0;
                //roundQueryCountMap.put(round, 0);
                int queryCount=0;
                for(int i = 0; i<VirtualHopsParameter.queryNumber; i++)
                {
                    queryCount++;
                    if(queryCount==10)
                    {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        queryCount=0;
                    }
                    Query query=new Query(1,5000 ,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(12000,15000,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(1,16,i);
                    collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
                    //LOG.info("Update round {}th query has been emitted!",i);
                    // System.out.println("Second round "+i+"th query has been emitted");
                    queryStartTime=System.currentTimeMillis();
                }
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.ModifiedAddQNodeToBNodeEdgesFinished))
        {
            this.skeletonGraphBuiltCount++;
            if(this.skeletonGraphBuiltCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
            {
               // long currentPureProcessingTime=(long) input.getValueByField(VirtualHopStreamFields.ModifiedQueryEdgesSendFinishFieldTime);
                queryStartTime=System.currentTimeMillis();
                this.QueryRound++;
                LOG.info("Modified query generate bolt: {}th round of query start time: {}", round,queryStartTime);
                //System.out.println("Query start time: "+queryStartTime);
                round=0;
                //roundQueryCountMap.put(round, 0);
                int queryCount=0;
                for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
                {
                    queryCount++;
                    if(queryCount==10)
                    {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        queryCount=0;
                    }
                    Query query=new Query(1,5000 ,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(12000,15000,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(5000,15000,i);
                    //Query query=new Query(1814,20126,i);
                    //Query query=new Query(1,16,i);
                    //query.k=(i+1)*2;
                    collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
                }
            }
        }

        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.QueryKSPResult))
        {
            // LOG.info("{} receive {}th query result!",boltID, resultCount);
            // System.out.println(boltID+" receive "+resultCount+"th query result!");
            resultCount++;
            List<Path> kspResultList=(List<Path>)input.getValueByField(VirtualHopStreamFields.KSPResult);
            totalProcessingTime=totalProcessingTime+(long)input.getValueByField(VirtualHopStreamFields.totalProcessingTimeQPBolt);
            int roundNumber=(int)input.getValueByField(VirtualHopStreamFields.roundNumber);
            //LOG.info("Single round number: {}",roundNumber);
            totalRoundNumber=totalRoundNumber+roundNumber;
            //  LOG.info("{} receives {} query results! Processing time is {}.",this.boltID,resultCount,(System.currentTimeMillis()-this.queryStartTime));
            if(resultCount==VirtualHopsParameter.queryNumber)
            {
                long endProcessingTime=System.currentTimeMillis();
                LOG.info("{}th round of query start time: {}", round,queryStartTime);
                LOG.info("{}th round of query end time: {}", round,endProcessingTime);
                LOG.info("Query number: {}", resultCount);
                //round++;
                resultCount=0;
                int averageLength=0;
                for(Path path:kspResultList)
                {
                    averageLength=averageLength+path.line.size();
                }
                averageLength=averageLength/kspResultList.size();
                LOG.info("{} th round of queries, average length of paths: {}", round,averageLength);
                long duringProcessingTime=endProcessingTime-this.queryStartTime;
                LOG.info("NewVersion: Total processing time in {}th round: {}",round,duringProcessingTime);
                LOG.info("NewVersion: communication time in {}th round: {}",round,duringProcessingTime-totalProcessingTime);
                int averageRoundNumber=totalRoundNumber/VirtualHopsParameter.queryNumber;
                LOG.info("Average round number: {}",averageRoundNumber);
                totalRoundNumber=0;


                if(this.updateFlag==false)
                {
                    this.updateFlag=true;
                    //collector.emit(VirtualHopStreamIDs.UpdateGraphTriggerFlag, new Values(true)); // this will trigger the sent of update graph from VirtualHopsConstructGraphBolt.
                    LOG.info("Update is beginning!");
                }
                if(this.QueryRound==5)
                {
                    //  collector.emit(VirtualHopStreamIDs.UpdateGraphTriggerFlag, new Values(true)); // this will trigger the sent of update graph from VirtualHopsConstructGraphBolt.
                }
                // The following code is to set different k values for every round
                int QK=0;
//                if(iterFlag==true && round<4)
//                {
//                    round++;
//                    int queryCount=0;
//                    for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
//                    {
//                        queryCount++;
//                        if(queryCount==10)
//                        {
//                            try {
//                                Thread.sleep(100);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                            queryCount=0;
//                        }
//                        //  Query query=new Query(9919,13026,i*(round+1));
//                        Query query=new Query(5100,21000 ,i);
//                        //Query query=new Query(5000,15000,i);
//                        //Query query=new Query(12000,15000,i);
//                        query.k=5+5*round;
//                        QK=query.k;
//                        collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
//                    }
//                    queryStartTime=System.currentTimeMillis();
//                    LOG.info("{}th round of queries are sent, k is {}", round,QK);
//                }

                //The following code is to set different query number for every round
//                if(round<10)
//                {
//                    for(int i=0;i<VirtualHopsParameter.queryNumber*(round+1);i++)
//                    {
//                        //  Query query=new Query(9919,13026,i*(round+1));
//                        Query query=new Query(12000,15000,i);
//                        //query.k=2+2*round;
//                        collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
//                    }
//                    queryStartTime=System.currentTimeMillis();
//                    LOG.info("{}th round of queries are sent", round);
//                }

            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(VirtualHopStreamIDs.KSPQueryToBeProcessed,
                new Fields(VirtualHopStreamFields.KSPQuery));

        declarer.declareStream(VirtualHopStreamIDs.ModifiedQueryToSubgraphBolt,
                new Fields(VirtualHopStreamFields.ModifiedQueryToSubgraphField));

        declarer.declareStream(VirtualHopStreamIDs.UpdateGraphTriggerFlag,
                new Fields(VirtualHopStreamFields.UpdateGraphTriggerFlagField));
    }
}
