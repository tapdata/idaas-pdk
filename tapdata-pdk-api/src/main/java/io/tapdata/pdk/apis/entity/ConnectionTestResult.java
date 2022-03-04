package io.tapdata.pdk.apis.entity;

import java.util.List;

public class ConnectionTestResult {
    public static final int RESULT_SUCCESSFULLY = 1;
    public static final int RESULT_SUCCESSFULLY_WITH_WARN = 2;
    public static final int RESULT_FAILED = 10;
    private int result;

    private List<TestItem> testItems;

    public static class TestItem {
        /**
         * Test item, like connection test, username and password test, etc
         */
        private String item;
        public static final String RESULT_PASS = "pass";
        public static final String RESULT_FAILED = "failed";
        /**
         * Test result, pass or failed
         */
        private String result;
        /**
         * Information about why it failed
         */
        private String information;

        public String getItem() {
            return item;
        }

        public void setItem(String item) {
            this.item = item;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public String getInformation() {
            return information;
        }

        public void setInformation(String information) {
            this.information = information;
        }

        @Override
        public String toString() {
            return TestItem.class.getSimpleName() + " item " + item +
                    " result " + result +
                    " information " + information;
        }
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public List<TestItem> getTestPoints() {
        return testItems;
    }

    public void setTestPoints(List<TestItem> testItems) {
        this.testItems = testItems;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(ConnectionTestResult.class.getSimpleName());
        switch (result) {
            case RESULT_FAILED:
                builder.append(" failed, ");
                break;
            case RESULT_SUCCESSFULLY:
                builder.append(" successfully, ");
                break;
            case RESULT_SUCCESSFULLY_WITH_WARN:
                builder.append(" successfully with warning, ");
                break;
            default:
                builder.append(" unexpected result ").append(result).append(", ");
                break;
        }
        if(testItems != null) {
            for(TestItem testItem : testItems) {
                builder.append(testItem.toString()).append("; ");
            }
        }
        return builder.toString();
    }
}
