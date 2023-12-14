package KSPOnStorm;

import KSPOnStorm.VirtualHopsData.VirtualHopsParameter;

import java.io.Serializable;

public class Query implements Serializable {
    public int sourceNodeID;
    public int endNodeID;
    public int k;
    public int queryID;
    public Query()
    {

    }
    public  Query(int sourceNodeID, int endNodeID)
    {
        this.sourceNodeID=sourceNodeID;
        this.endNodeID=endNodeID;
        this.k=VirtualHopsParameter.queryK;
    }
    public  Query(int sourceNodeID, int endNodeID, int queryID)
    {
        this.sourceNodeID=sourceNodeID;
        this.endNodeID=endNodeID;
        this.k=VirtualHopsParameter.queryK;
        this.queryID=queryID;
    }
}
