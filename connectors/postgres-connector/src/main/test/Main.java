import java.util.Arrays;
import java.util.List;

/**
 * @author Administrator
 * @date 2022/4/29
 */
public class Main {
    public static void main(String[] args) {
        List<String> a = Arrays.asList("a","b");
        a.forEach(k -> {
            if("a".equals(k))
                return;
            System.out.println(k);
        });
    }
}
