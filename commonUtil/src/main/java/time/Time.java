package time;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

public class Time {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        Date time1 = new Date(1516310761);//Long--->Date
        String time2 = sdf.format(time1);//Date--->String
        System.out.println(time2);
        LocalDateTime localDateTime;
    }
}
