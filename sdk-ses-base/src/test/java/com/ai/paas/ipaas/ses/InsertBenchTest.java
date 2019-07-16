package com.ai.paas.ipaas.ses;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchCmpClientFactory;

public class InsertBenchTest {
    private Logger logger = LoggerFactory.getLogger(InsertBenchTest.class);
    static ISearchClient client = null;
    static String indexName = "user";
    static String mapping = null;

    @BeforeClass
    public static void setUpBeforeClass() {
        String hosts = "localhost:9200";
        String id = "userId";
        client = SearchCmpClientFactory.getSearchClient(hosts, indexName, id, "elastic", "123456");
    }

    @AfterClass
    public static void tearDownAfterClass() {
        client = null;
    }

    @Test
    public void testBulkInsert() {
        long start = System.currentTimeMillis();
        List<User> data = new ArrayList<>();
        User user = null;
        for (int i = 0; i < 100000; i++) {
            user = new User("" + i, "Batch insert i:" + i, i, new Date());
            data.add(user);
            if (i % 1000 == 0) {
                client.bulkInsert(data);
                data.clear();
            }
        }
        client.bulkInsert(data);
        // 本机测试10万条5595毫秒
        logger.info("Total time for 100000 is {}", (System.currentTimeMillis() - start));
    }

    @Test
    public void testScrollSearch() {
        long start = System.currentTimeMillis();
        String r = client.searchBySQL("age:(>=50000 AND <60000) ", 30, 10, null);
        logger.info("Search time is {},{}", (System.currentTimeMillis() - start), r);
        start = System.currentTimeMillis();
        r = client.searchBySQL("age:(>=50000 AND <60000) ", 100, 10, null);
        logger.info("Total time for scroll is {},{}", (System.currentTimeMillis() - start), r);
        start = System.currentTimeMillis();
        r = client.searchBySQL("age:(>=50000 AND <60000) ", 500, 10, null);
        logger.info("Total time for scroll is {},{}", (System.currentTimeMillis() - start), r);
    }
}
