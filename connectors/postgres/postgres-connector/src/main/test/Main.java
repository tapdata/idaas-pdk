import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    public static void main(String[] args) {
        String patten = "HH:mm:ss.SSS";
        SimpleDateFormat dateFormat = new SimpleDateFormat(patten);
        System.out.println(dateFormat.format(new Date()));
    }
}
