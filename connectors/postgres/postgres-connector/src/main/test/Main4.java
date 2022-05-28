import io.tapdata.entity.utils.DataMap;

public class Main4 {
    public static void main(String[] args) {
        DataMap dataMap = new DataMap();
        dataMap.put("aaa", null);
        System.out.println(dataMap.getString("aaa").length());
    }
}
