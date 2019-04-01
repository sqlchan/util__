package apache;

import java.io.*;

public class St {
    public static void main(String[] args) throws IOException {
        BufferedReader br= new BufferedReader(new FileReader(new File("E:\\util\\string_apache\\src\\main\\resources\\b.txt")));
        BufferedWriter bw= new BufferedWriter(new FileWriter(new File("E:\\util\\string_apache\\src\\main\\resources\\d.txt")));
        String s;

        while ((s= br.readLine())!=null){
            StringBuilder sb=new StringBuilder();
            if (!s.contains("unknown")) {
                sb.append(s);
                sb.append("\n");
                bw.write(sb.toString());
                bw.flush();
            }
//            int a1=s.indexOf(":");
//            int a2=s.indexOf(",",a1+1);
//            String s1=s.substring(a1+2,a2);
//            int b1=s.indexOf(":",a2+1);
//            int b2=s.indexOf(",",b1+1);
//            String s2=s.substring(b1+2,b2);
//            int c1=s.indexOf(":",b2+1);
//            int c2=s.indexOf("的");
//            String s3=s.substring(c1+2,c2);
//            String str1="<notice softwareName=\"";
//            String str2="\" softwareVersion=\"";
//            String str3="\" licenseId=\"";
//            String str4="\" />";
//            sb.append(str1);
//            sb.append(s1);
//            sb.append(str2);
//            sb.append(s2);
//            sb.append(str3);
//            sb.append(s3);
//            sb.append(str4);
//            sb.append("\n");
//            bw.write(sb.toString());bw.flush();
        }

    }
}
