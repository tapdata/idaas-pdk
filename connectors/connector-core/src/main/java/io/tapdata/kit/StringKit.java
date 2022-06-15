package io.tapdata.kit;

import java.util.List;
import java.util.stream.Collectors;

public class StringKit {

    /**
     * write string several times
     *
     * @param copied   "?"
     * @param count    3
     * @param combiner ","
     * @return "?,?,?"
     */
    public static String copyString(String copied, Integer count, String combiner) {
        if (count < 1 || EmptyKit.isNull(copied)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(copied).append(combiner);
        }
        return sb.delete(sb.length() - combiner.length(), sb.length()).toString();
    }

    //replace first
    public static String replaceOnce(String text, String searchString, String replacement) {
        if (EmptyKit.isEmpty(text)) {
            return "";
        }
        return text.replace(searchString, replacement);
    }

    /**
     * join strings with around and splitter
     *
     * @param list     ["a","b","c"]
     * @param around   "'"
     * @param splitter ","
     * @return "'a','b','c'"
     */
    public static String joinString(List<String> list, String around, String splitter) {
        if (EmptyKit.isEmpty(list)) {
            return "";
        }
        return list.stream().map(s -> around + s + around).collect(Collectors.joining(splitter));
    }

//    public static int compareVersion(String version1, String version2) {
//        version1.split(".")
//    }

}
