package io.tapdata.pdk.core.api.impl;

import io.tapdata.pdk.apis.utils.TapUtils;
import io.tapdata.pdk.core.annotations.Implementation;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.util.concurrent.TimeUnit;

@Implementation(TapUtils.class)
public class TapUtilsImpl implements TapUtils {

    @Override
    public void interval(Runnable runnable, int seconds) {
        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(runnable, seconds, TimeUnit.SECONDS);
    }



    public static void main(String... args) {
//        String str = new TapUtilsImpl().format("{}sadfdf{} sakldfjlaf{}", "a{}a", "{}}", "d{}d");
//        System.out.println("str " + str);
//        String str = "curl \"https://api.vika.cn/fusion/v1/datasheets/dstkATXeNb8lCMamtV/records?viewId=viwRPmM7p1WYJ&fieldKey=name\" \\\n" +
//                "  -H \"Authorization: Bearer uskMiSCZAbukcGsqOfRqjZZ\"\n" +
//                "\n";
//        CUrl.main(str.split("\n"));
//        CUrl curl = new CUrl("https://api.vika.cn/fusion/v1/datasheets/dstkUHm8j0UlsywXW2/records?viewId=viwj9vhXgxXmm&fieldKey=name")
//                .header("Authorization: Bearer uskMiSCZAbukcGsqOfRqjZZ");

//        CUrl curl = new CUrl("https://api.vika.cn/fusion/v1/datasheets/dstkATXeNb8lCMamtV/records?viewId=viwRPmM7p1WYJ&fieldKey=name")
//                .header("Authorization: Bearer uskMiSCZAbukcGsqOfRqjZZ")
//                .header("Content-Type: application/json")
//                .data("{\n" +
//                        "  \"records\": [\n" +
//                        "  {\n" +
//                        "    \"fields\": {\n" +
//                        "      \"标题\": \"的点点滴滴\",\n" +
//                        "      \"选项\": [\n" +
//                        "        \"111\"\n" +
//                        "      ]\n" +
//                        "    }\n" +
//                        "  },\n" +
//                        "  {\n" +
//                        "    \"fields\": {\n" +
//                        "      \"标题\": \"22\",\n" +
//                        "      \"选项\": [\n" +
//                        "        \"111\"\n" +
//                        "      ]\n" +
//                        "    }\n" +
//                        "  }\n" +
//                        "],\n" +
//                        "  \"fieldKey\": \"name\"\n" +
//                        "}");
//        String result = curl.exec("utf8");
//        System.out.println("result " + result);
    }
}
