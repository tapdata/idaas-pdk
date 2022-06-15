package io.tapdata.kit;

import java.util.Arrays;
import java.util.Iterator;
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

    public static int compareVersion(String version1, String version2) {
        List<String> list1 = Arrays.stream(version1.split("\\.")).collect(Collectors.toList());
        List<String> list2 = Arrays.stream(version2.split("\\.")).collect(Collectors.toList());
        Iterator<String> iterator1 = list1.iterator();
        Iterator<String> iterator2 = list2.iterator();
        while (iterator1.hasNext() && iterator2.hasNext()) {
            String str1 = iterator1.next();
            String str2 = iterator2.next();
            if (Integer.parseInt(str1) > Integer.parseInt(str2)) {
                return 1;
            } else if (Integer.parseInt(str1) < Integer.parseInt(str2)) {
                return -1;
            }
        }
        if (iterator1.hasNext()) {
            return 1;
        } else if (iterator2.hasNext()) {
            return -1;
        } else {
            return 0;
        }
    }

}
