package ranks;

// import library
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Ranks{

    public static void main(String[] args){
        // set k
        int k=10000;
        String filePath="input/Ranks.csv";
                createRanks(k,filePath);
    }

    public static void createRanks(int k,String filePath){
        try (BufferedWriter bw=new BufferedWriter(new FileWriter(filePath))){
            double initialRank=1.0/(k*k);

            for (int page=1;page<=k*k;page++){
                bw.write(page+","+initialRank);
                bw.newLine();
            }

            // for dummy page
            bw.write("0,0");
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}