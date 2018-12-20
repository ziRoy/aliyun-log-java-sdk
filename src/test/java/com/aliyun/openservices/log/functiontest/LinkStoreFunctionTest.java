package com.aliyun.openservices.log.functiontest;

import com.aliyun.openservices.log.common.LinkStore;
import com.aliyun.openservices.log.common.LogStore;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.GetLogStoreResponse;
import com.aliyun.openservices.log.response.ListLogStoresResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.Assert.assertEquals;

public class LinkStoreFunctionTest extends FunctionTest {
    private static final String TEST_PROJECT = "project-link-ft";
    private static final String TEST_LOG_STORE = "logstore-link-ft";
    private static final String TEST_LINK_STORE = "linkstore-link-ft";

    @Before
    public void setUp()
    {
        safeCreateProject(TEST_PROJECT, "");
        safeDeleteLogStore(TEST_PROJECT, TEST_LOG_STORE);
        safeDeleteLinkStore(TEST_PROJECT, TEST_LINK_STORE);
    }

    @Test
    public void testLinkStore() throws LogException {
        LogStore logStore = new LogStore(TEST_LOG_STORE, 3, 2);
        client.CreateLogStore(TEST_PROJECT, logStore);

        {
            GetLogStoreResponse response = client.GetLogStore(TEST_PROJECT, TEST_LOG_STORE);
            LogStore logStore1 = response.GetLogStore();
            assertEquals(3, logStore1.GetTtl());
            assertEquals(2, logStore1.GetShardCount());
            assertEquals(TEST_LOG_STORE, logStore1.GetLogStoreName());
        }

        LinkStore linkStore = new LinkStore(TEST_LINK_STORE, TEST_PROJECT, TEST_LOG_STORE);
        client.CreateLinkStore(TEST_PROJECT, linkStore);
        {
            GetLogStoreResponse response = client.GetLogStore(TEST_PROJECT, TEST_LINK_STORE);
            LogStore linkStore1 = response.GetLogStore();
            assertEquals(3, linkStore1.GetTtl());
            assertEquals(2, linkStore1.GetShardCount());
            assertEquals(TEST_LINK_STORE, linkStore1.GetLogStoreName());
            assertEquals(true, linkStore1.IsLink());
            assertEquals(TEST_PROJECT, linkStore1.GetSourceProject());
            assertEquals(TEST_LOG_STORE, linkStore1.GetSourceLogStore());
        }
        {
            ListLogStoresResponse response = client.ListLogStores(TEST_PROJECT, 0, 10, "");
            ArrayList<String> list = response.GetLogStores();
            assertEquals(2, list.size());
            assertEquals(TEST_LINK_STORE, list.get(0));
            assertEquals(TEST_LOG_STORE, list.get(1));
        }
    }

    @After
    public void tearDown()
    {
        safeDeleteLogStore(TEST_PROJECT, TEST_LOG_STORE);
        safeDeleteLinkStore(TEST_PROJECT, TEST_LINK_STORE);
        safeDeleteProject(TEST_PROJECT);
    }


}
