import java.util.Arrays;
import java.util.List;

public class Main2 {

    public static void main(String[] args) {
        List<String> tables = Arrays.asList("Student", "Teacher");
        System.out.println(tables.stream().reduce((v1, v2)-> "'" + v1 + "','" + v2 + "'").orElseGet(String::new));
    }
}
