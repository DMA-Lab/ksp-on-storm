package KSPOnStorm.VirtualHopsBolt;

import KSPOnStorm.Path;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamFields;
import KSPOnStorm.VirtualHopsData.VirtualHopStreamIDs;
import KSPOnStorm.Query;
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
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualHopsQueryGenerateBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualHopsQueryGenerateBolt.class);

    OutputCollector collector;
    List<Query> generatedQueryList;
    String boltID=null;
    long queryStartTime=0;
    int resultCount=0;
//    Map<Integer, Integer> roundQueryCountMap=null;
    int round=0;
    int skeletonGraphBuiltCount=0;
    int skeletonGraphUpdateCount=0;
    int QueryRound=0;
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
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.QueProBoltUpdateSkeGraFinish))
        {
            this.skeletonGraphUpdateCount++;
            if(this.skeletonGraphUpdateCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
            {
                this.QueryRound++;
                LOG.info("Update query start time: {}", queryStartTime);
               // System.out.println("Second round query start time: "+queryStartTime);
                round=0;
                //roundQueryCountMap.put(round, 0);
                for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
                {
                    //Query query=new Query(3442,6084,i);
                    Query query=new Query(1,11334,i);
                    //Query query=new Query(1814,20126,i);
                    //Query query=new Query(1,16,i);
                    //query.k=(i+1)*2;
                    collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
                    LOG.info("Update round {}th query has been emitted!",i);
                   // System.out.println("Second round "+i+"th query has been emitted");
                    queryStartTime=System.currentTimeMillis();
                }
            }
        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.SkeletonGraphBuilt))
        {
            this.skeletonGraphBuiltCount++;
            if(this.skeletonGraphBuiltCount==VirtualHopsBoltNumberConfig.VirtualHopsQueryProcessingBoltNumber)
            {
                this.QueryRound++;
                LOG.info("{}th round of query start time: {}", round,queryStartTime);
                //System.out.println("Query start time: "+queryStartTime);
                round=0;
                //roundQueryCountMap.put(round, 0);
                for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
                {
                    Query query=new Query(1,11334,i);
                    //Query query=new Query(3442,6084,i);
                    //Query query=new Query(1814,20126,i);
                    //Query query=new Query(1,16,i);
                    //query.k=(i+1)*2;
                    collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
               //     LOG.info("{}th query has been emitted!",i);
               //     System.out.println(i+"th query has been emitted");
                    queryStartTime=System.currentTimeMillis();
                }
            }
        }
//        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.QuerySend_Flag))
//        {
//            queryStartTime=System.currentTimeMillis();
//            LOG.info("query start time: {}", queryStartTime);
//            System.out.println("Query start time: "+queryStartTime);
//            round=0;
//            //roundQueryCountMap.put(round, 0);
//            for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
//            {
//               // Query query=new Query(9919,13026,i);
//                Query query=new Query(12732,190806,i);
//                //query.k=(i+1)*2;
//                collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
//                LOG.info("{}th query has been emitted!",i);
//                System.out.println(i+"th query has been emitted");
//            }
//        }
        if(input.getSourceStreamId().equals(VirtualHopStreamIDs.QueryKSPResult))
        {
           // LOG.info("{} receive {}th query result!",boltID, resultCount);
           // System.out.println(boltID+" receive "+resultCount+"th query result!");
            resultCount++;
            List<Path> kspResultList=(List<Path>)input.getValueByField(VirtualHopStreamFields.KSPResult);
          //  LOG.info("{} receives {} query results! Processing time is {}.",this.boltID,resultCount,(System.currentTimeMillis()-this.queryStartTime));
            if(resultCount==VirtualHopsParameter.queryNumber)
            {
                round++;
                long endProcessingTime=System.currentTimeMillis();
                LOG.info("{}th round of query start time: {}", round,queryStartTime);
                LOG.info("{}th round of query end time: {}", round,endProcessingTime);

                resultCount=0;
                int averageLength=0;
                for(Path path:kspResultList)
                {
                    averageLength=averageLength+path.line.size();
                }
                averageLength=averageLength/kspResultList.size();
                LOG.info("{} th round of queries, average length of paths: {}", round,averageLength);
               // System.out.println("Query start time: "+queryStartTime);
               // System.out.println("Query end time: "+endProcessingTime);
                long duringProcessingTime=endProcessingTime-this.queryStartTime;
                LOG.info("Total processing time in {}th round: {}",round,duringProcessingTime);
                //String outPutLine="Total query processing time in "+round+"th round: "+duringProcessingTime+". AverageLength is "+averageLength;
                //System.out.println(outPutLine);
               // LOG.info("Write the result to local file!");
               // writeRateGraph(outPutLine,round);
                if(this.QueryRound==5)
                {
                  //  collector.emit(VirtualHopStreamIDs.UpdateGraphTriggerFlag, new Values(true)); // this will trigger the sent of update graph from VirtualHopsConstructGraphBolt.
                }
                if(round<5)
                {
                    for(int i=0;i<VirtualHopsParameter.queryNumber;i++)
                    {
                      //  Query query=new Query(9919,13026,i*(round+1));
                        Query query=new Query(1,11334,i);
                        query.k=2+2*round;
                        collector.emit(VirtualHopStreamIDs.KSPQueryToBeProcessed,new Values(query));
                    }
                    queryStartTime=System.currentTimeMillis();
                    LOG.info("{}th round of queries are sent", round);
                }
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(VirtualHopStreamIDs.KSPQueryToBeProcessed,
                new Fields(VirtualHopStreamFields.KSPQuery));

        declarer.declareStream(VirtualHopStreamIDs.UpdateGraphTriggerFlag,
                new Fields(VirtualHopStreamFields.UpdateGraphTriggerFlagField));
    }
    public void writeRateGraph(String queryProcessingTime, int round)
    {
        //String writeAddress="C:\\Users\\esouser\\Desktop\\Graph\\result.txt";
        String writeAddress="/root/testData/result.txt";
        System.out.println("Writing starts:");
        try {
            File file = new File(writeAddress);
            if(!file.exists())
            {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file,true);
            BufferedWriter bw = new BufferedWriter(fw);
            String line=round+"th queries processing time: "+queryProcessingTime;
            bw.write(line);
            bw.write("\n");
            System.out.println("Writing is finished!");
            bw.close();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }
    }
}
