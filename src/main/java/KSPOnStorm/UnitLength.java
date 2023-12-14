package KSPOnStorm;

import java.awt.*;

public class UnitLength implements Comparable {
    double newWeight;
    int unitNumber;
    double unitAverageWeight;
    public UnitLength()
    {}
    public UnitLength(double newWeight, int unitNumber)
    {
        this.newWeight=newWeight;
        this.unitNumber=unitNumber;
        this.unitAverageWeight=newWeight/unitNumber;
    }
    @Override
    public int compareTo(Object arg0) {
        // TODO Auto-generated method stub
        UnitLength nextUL=(UnitLength)arg0;
        return Double.compare(this.unitAverageWeight,nextUL.unitAverageWeight);
    }
    public void outPutString()
    {
        String ulString="unitNumber: "+this.unitNumber+", unitWeight: "+this.newWeight;
        System.out.println(ulString);
    }
}
