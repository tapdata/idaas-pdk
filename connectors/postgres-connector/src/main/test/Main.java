import io.tapdata.postgres.PostgresCdcRunner;
import io.tapdata.postgres.config.PostgresConfig;

import java.util.Collections;

/**
 * @author Administrator
 * @date 2022/4/29
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        List<String> a = Arrays.asList("a","b");
//        a.forEach(k -> {
//            if("a".equals(k))
//                return;
//            System.out.println(k);
//        });
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        Class.forName("org.postgresql.Driver");
//        Connection conn = DriverManager.getConnection("jdbc:postgresql://192.168.1.189:5432/coolgj", "postgres", "abc123");
//        PreparedStatement statement = conn.prepareStatement("insert into public.ccc values (?)");
//        statement.setObject(1,"B");
//        statement.addBatch();
//        statement.setObject(1,"AC");
//        statement.addBatch();
//        statement.executeBatch();
//        statement.setObject(1, new java.sql.Timestamp(123123123123L));
//        statement.execute();
//        statement.close();
//        DatabaseMetaData databaseMetaData = conn.getMetaData();
//        ResultSet resultSet = databaseMetaData.getIndexInfo("coolgj", "public", "bbb", false, false);
//        resultSet.first();
//        while(resultSet.next()) {
//            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
//            for(int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
//                System.out.println(resultSetMetaData.getColumnName(i+1) + ":" + resultSet.getObject(i+1));
//            }
//        }
//        conn.close();
//        StringBuilder stringBuilder = new StringBuilder();
//        TapAdvanceFilter filter = new TapAdvanceFilter();
//        filter.setLimit(100);
//        filter.setSkip(50);
//        DataMap dataMap = new DataMap();
//        dataMap.put("k1", "v1");
//        dataMap.put("k2", "v2");
//        filter.setMatch(dataMap);
//        List<QueryOperator> list = new ArrayList<>();
//        list.add(new QueryOperator("k3", "v3", 4));
//        list.add(new QueryOperator("k4", "v4", 1));
//        filter.setOperators(list);
//        List<SortOn> list2 = new ArrayList<>();
//        list2.add(new SortOn("k1", 2));
//        list2.add(new SortOn("k3", 1));
//        filter.setSortOnList(list2);
//        stringBuilder.append("SELECT * FROM \"").append("TAble1").append("\" ");
//
//        System.out.println(stringBuilder.append(SqlBuilder.buildSqlByAdvanceFilter(filter)));

        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("192.168.1.189");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("COOLGJ");
        postgresConfig.setSchema("public");
        postgresConfig.setExtParams("");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        PostgresCdcRunner runner = new PostgresCdcRunner(postgresConfig,
//                Collections.singletonList("Student")
                null
        );
        runner.registerConsumer(null, 0, null);
//        DebeziumCdcPool.addRunner(runner.getRunnerName(), runner);
        runner.startCdcRunner();


//        Thread.sleep(20000);
//        runner.closeCdc();
//        new Thread(() -> PostgresCdcPool.removeRunner(runner.getSlotName())).start();

//        runner.closeCdc();
//        System.out.println("SB");

//        Count count = new Count();
//        new Thread(count).start();
//        Thread.sleep(200);
//        count.stop();
//        while(true) {
//            Thread.sleep(10000);
//            ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
//            //activeCount()返回当前正在活动的线程的数量
//            int total = Thread.activeCount();
//            Thread[] threads = new Thread[total];
//            //enumerate(threads)将当前线程组中的active线程全部复制到传入的线程数组threads中
//            // 并且返回数组中元素个数，即线程组中active线程数量
//            threadGroup.enumerate(threads);
//            for (Thread t:threads){
//                System.out.println(TapSimplify.toJson(t));
//            }
//        }
//        System.exit(0);
    }
}
