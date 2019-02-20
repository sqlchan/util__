package apache;

import java.io.*;

public class fileRead {
    public static void main(String[] args) throws IOException {
        File file=new File("D:\\code\\165\\xwifi-streaming\\xwifi.xwifi-streaming_other.log");
        Reader reader=new BufferedReader(new FileReader(file));
        String s ="";
        while ((s=((BufferedReader) reader).readLine()) != null){
            if (s.indexOf("时间") != -1){
                System.out.println(s);
            }
        }
    }
}
