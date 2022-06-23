import java.text.SimpleDateFormat;
import java.util.Date;

public class Main2 {
    public static void main(String[] args) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("DD-MON-RR");
        System.out.println(simpleDateFormat.format(new Date()));
    }
}
