package io.tapdata.kit;

public class StringKit {

    public static String copyString(String copied, Integer count, String combiner) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(copied).append(combiner);
        }
        return sb.delete(sb.length() - combiner.length(), sb.length()).toString();
    }

}
