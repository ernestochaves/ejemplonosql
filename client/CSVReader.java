package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

public class CSVReader {
    public static final String delimiter = ",";

    public CSVReader() {
        super();
    }

    public static List<People> read(String csvFile) {
        List<People> result = new ArrayList<People>();
        try {

            File file = new File(csvFile);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String[] tempArr;
            //Skip one line
            br.readLine();
            while ((line = br.readLine()) != null) {
                People p = new People();
                tempArr = line.split(delimiter);

                p.setId(tempArr[0]);
                p.setFirstName(tempArr[1]);
                p.setLastName(tempArr[2]);
                p.setGender(tempArr[3]);
                p.setBirthDate(tempArr[4]);
                p.setPhoneNumber(tempArr[5]);
                p.setEmail(tempArr[6]);
                result.add(p);

            }
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return result;
    }
}
