package KSPOnStorm;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class WriteResultsIntoFile {

    public void writeResult(long Time, String note)
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
            String line=note+Time;
            bw.write(line);
            bw.write("\n");
            System.out.println("Writing is finished!");
            bw.close();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }

        // ---
        //Runtime.getRuntime().
    }
}
