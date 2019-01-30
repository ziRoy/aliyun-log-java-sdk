package com.aliyun.openservices.log.sample;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.ListProjectConsumerGroupResponse;
import com.aliyun.openservices.log.response.ListShardResponse;
import com.aliyun.openservices.log.response.ProjectConsumerGroupCheckPointResponse;
import com.aliyun.openservices.log.response.ProjectConsumerGroupHeartBeatResponse;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * View Sample
 *
 * @author mingming.kmm
 * @date 2019-01-22 11:15 AM
 */
public class ViewSample {
    private Client client;
    private final String consumerGroup = "cg-all";
    private final String consumerId = "wujing";
    private final String view = "test-view";
    private final String expr = "*";

    private ViewSample(final String endpoint, final String accessId, final String accessKey) {
        client = new Client(endpoint, accessId, accessKey);
    }

    public static void main(String[] args) {
        String accessId = "94to3z418yupi6ikawqqd370";
        String accessKey = "DFk3ONbf81veUFpMg7FtY0BLB2w=";
        String endpoint = "cn-hangzhou-devcommon-intranet.sls.aliyuncs.com";
        ViewSample sample = new ViewSample(endpoint, accessId, accessKey);
        sample.run();
    }

    private void run() {
        System.out.println("\n------------- create project and link -----------");
        createProject("test-project", "test 0");
        createProject(view, "test 1");
        createLogStore("test-project", "test-logstore-0");
        createLogStore("test-project", "test-logstore-1");
        createLink("test-project", "test-logstore-0", view, "link-0");
        createLink("test-project", "test-logstore-1", view, "link-1");

        System.out.println("\n------------- wait project seen by cgi -----------");
        waitForSec(125);
        System.out.println("\n------------- write some test log ----------------");
        writeSomeLog("test-project", "test-logstore-0");
        writeSomeLog("test-project", "test-logstore-1");

        System.out.println("\n------ create/update consumer group on view ------");
        createProjectConsumerGroup(view, consumerGroup, expr, 60, true);
        ArrayList<ProjectConsumerGroup> consumergroups = getProjectConsumerGroup(view);
        printConsumerGroup(consumergroups);
        updateProjectConsumerGroup(view, consumerGroup, 30, false);
        consumergroups = getProjectConsumerGroup(view);
        printConsumerGroup(consumergroups);
        updateProjectConsumerGroup(view, consumerGroup, 120, true);
        consumergroups = getProjectConsumerGroup(view);
        printConsumerGroup(consumergroups);

        System.out.println("\n----------------- wait shard heart beat --------------");
        waitForSec(70);
        System.out.println("\n------------------ get checkpoint --------------------");
        Map<String, ArrayList<ConsumerGroupShardCheckPoint>> cps = getCheckPoints(view, "", consumerGroup, -1);
        printListMap(cps);
        cps = getCheckPoints(view, "link-0", consumerGroup, -1);
        printListMap(cps);
        cps = getCheckPoints(view, "link-1", consumerGroup, 0);
        printListMap(cps);

        // start heart beat
        System.out.println("\n----------------- shard heart beat -------------------");
        Map<String, ArrayList<Integer>> beatShards = heartBeat(view, consumerGroup, consumerId, new HashMap<String, ArrayList<Integer>>());
        printListMap(beatShards);
        beatShards = heartBeat(view, consumerGroup, consumerId, beatShards);
        printListMap(beatShards);

        System.out.println("\n----------------- split shard -----------------------");
        // split shard 0
        splitShard("test-project", "test-logstore-0", 0);

        shardHeartBeatSomeTimes(view, consumerGroup, consumerId, beatShards, 2, 30);

        // get shard 0 endcursor
        System.out.println("\n----------------- get begin cursor -------------------");
        String beginCursor = getBeginCursor(view, "link-0", 0);
        System.out.println("end cursor: " + beginCursor);
        System.out.println("\n-------------- update check point to begin -----------");
        updateCheckPoint(view, consumerGroup, consumerId, "link-0", 0, beginCursor);

        System.out.println("\n------------------ get checkpoint --------------------");
        cps = getCheckPoints(view, "", consumerGroup, -1);
        printListMap(cps);

        shardHeartBeatSomeTimes(view, consumerGroup, consumerId, beatShards, 4, 30);

        System.out.println("\n------------------ get checkpoint --------------------");
        cps = getCheckPoints(view, "", consumerGroup, -1);
        printListMap(cps);

        System.out.println("\n----------------- get end cursor ---------------------");
        String endCursor = getEndCursor(view, "link-0", 0);
        System.out.println("end cursor: " + endCursor);
        System.out.println("\n-------------- update check point to end --------------");
        updateCheckPoint(view, consumerGroup, consumerId, "link-0", 0, endCursor);

        shardHeartBeatSomeTimes(view, consumerGroup, consumerId, beatShards, 4, 30);

        System.out.println("\n------------------ get checkpoint --------------------");
        cps = getCheckPoints(view, "", consumerGroup, -1);
        printListMap(cps);

        // update checkpoint to begin cursor
        beginCursor = getBeginCursor(view, "link-0", 1);
        updateCheckPoint(view, consumerGroup, consumerId, "link-0", 1, beginCursor);
        beginCursor = getBeginCursor(view, "link-0", 2);
        updateCheckPoint(view, consumerGroup, consumerId, "link-0", 2, beginCursor);
        beginCursor = getBeginCursor(view, "link-0", 3);
        updateCheckPoint(view, consumerGroup, consumerId, "link-0", 3, beginCursor);
        beginCursor = getBeginCursor(view, "link-1", 0);
        updateCheckPoint(view, consumerGroup, consumerId, "link-1", 0, beginCursor);
        beginCursor = getBeginCursor(view, "link-1", 1);
        updateCheckPoint(view, consumerGroup, consumerId, "link-1", 1, beginCursor);

        System.out.println("\n------------------ get checkpoint --------------------");
        cps = getCheckPoints(view, "", consumerGroup, -1);
        printListMap(cps);
    }

    private void shardHeartBeatSomeTimes(final String project, final String consumerGroup, final String consumerId, Map<String, ArrayList<Integer>> beatShards, int times, int gap) {
        while (times-- > 0) {
            System.out.println("\n----------------- shard heart beat -------------------");
            beatShards = heartBeat(project, consumerGroup, consumerId, beatShards);
            printListMap(beatShards);
            waitForSec(gap);
        }
    }

    private List<LogItem> genLogGroup(int num) {
        List<LogItem> group = new ArrayList<LogItem>();
        for (int i = 0; i < num; ++i) {
            LogItem item = new LogItem();
            item.PushBack(new LogContent("content", "test content + " + System.currentTimeMillis()));
            group.add(item);
        }
        return group;
    }

    private void writeSomeLog(final String project, final String logstore) {
        try {
            for (int i = 0; i < 20; ++i) {
                client.PutLogs(project, logstore, "", genLogGroup(10), "127.0.0.1");
            }
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private void waitForSec(int sec) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sec));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private <T> void printListMap(Map<String, ArrayList<T>> cps) {
        System.out.println("content [");
        if (cps != null) {
            for (Map.Entry<String, ArrayList<T>> e : cps.entrySet()) {
                System.out.println("    logstore: [" + e.getKey());
                for (T c : e.getValue()) {
                    System.out.println("        " + c);
                }
                System.out.println("    ]");
            }
        } else {
            System.out.println("    null");
        }
        System.out.println("]");
    }

    private void printConsumerGroup(ArrayList<ProjectConsumerGroup> groups) {
        if (groups == null) {
            return;
        }
        System.out.println("groups: [");
        for (ProjectConsumerGroup group : groups) {
            System.out.println(group);
        }
        System.out.println("]");
    }

    private String getMidHash(final String includeBegin, final String excludeEnd) {
        if (includeBegin.length() != 32 || excludeEnd.length() != 32) {
            return null;
        }
        long incBegin = Long.parseLong(includeBegin.substring(0, 8), 16);
        long excEnd = Long.parseLong(excludeEnd.substring(0, 8), 16);

        long mid = (excEnd - incBegin + 1) / 2 + incBegin;
        String midstr = Long.toHexString(mid);

        StringBuilder sb = new StringBuilder(midstr);

        while (sb.length() < 8) {
            sb.insert(0, '0');
        }

        while (sb.length() < 32) {
            sb.append(midstr.charAt(midstr.length() - 1));
        }
        return sb.toString();
    }

    private void splitShard(final String project, final String logstore, int shard) {
        try {
            ListShardResponse shards = client.ListShard(project, logstore);
            for (Shard s : shards.GetShards()) {
                if (s.GetShardId() == shard) {
                    client.SplitShard(project, logstore, shard, getMidHash(s.getInclusiveBeginKey(), s.getExclusiveEndKey()));
                }
            }

        } catch (LogException e) {
            e.printStackTrace();
        }
    }

    private String getEndCursor(final String project, final String logstore, int shard) {
        return getCursor(project, logstore, shard, Consts.CursorMode.END);
    }

    private String getBeginCursor(final String project, final String logstore, int shard) {
        return getCursor(project, logstore, shard, Consts.CursorMode.BEGIN);
    }

    private String getCursor(final String project, final String logstore, int shard, Consts.CursorMode mode) {
        try {
            return client.GetCursor(project, logstore, shard, mode).GetCursor();
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
        return null;
    }

    private void updateCheckPoint(final String project, final String consumerGroup, final String consumerId, final String logstore, int shard, final String checkpoint) {
        try {
            client.UpdateProjectConsumerGroupCheckPoint(project, consumerGroup, consumerId, logstore, shard, checkpoint);
            System.out.printf("update consumer group checkpoint %s %s %s %s %d success\n", project, consumerGroup, consumerId, logstore, shard);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private Map<String, ArrayList<ConsumerGroupShardCheckPoint>> getCheckPoints(final String project, final String logstore, final String consumerGroup, int shard) {
        try {
            ProjectConsumerGroupCheckPointResponse rsp = client.GetProjectConsumerGroupCheckPoint(project, consumerGroup, logstore, shard);
            System.out.printf("get consumer group checkpoints %s %s %s %d success\n", project, logstore, consumerGroup, shard);
            return rsp.getCheckPoints();
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
        return null;
    }

    private void updateProjectConsumerGroup(final String project, final String consumerGroup, int timeout, boolean order) {
        try {
            client.UpdateProjectConsumerGroup(project, consumerGroup, order, timeout);
            System.out.printf("update ConsumerGroup %s %s success\n", project, consumerGroup);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private Map<String, ArrayList<Integer>> heartBeat(final String project, final String consumerGroup, final String consumerId, final Map<String, ArrayList<Integer>> shards) {
        try {
            System.out.println("beat shards: ");
            printListMap(shards);
            ProjectConsumerGroupHeartBeatResponse rsp = client.ProjectConsumerGroupHeartBeat(project, consumerGroup, consumerId, shards);
            System.out.printf("heart beat %s %s success\n", project, consumerGroup);
            return rsp.getLogStoreShards();
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
        return null;
    }

    private ArrayList<ProjectConsumerGroup> getProjectConsumerGroup(final String project) {
        try {
            ListProjectConsumerGroupResponse rsp = client.ListProjectConsumerGroup(project);
            System.out.printf("get ConsumerGroup %s success\n", project);
            return rsp.getConsumerGroups();
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
        return null;
    }

    private void createProjectConsumerGroup(final String project, final String consumerGroup, final String expr, int timeout, boolean order) {
        ProjectConsumerGroup group = new ProjectConsumerGroup(consumerGroup, expr, timeout, order);
        try {
            client.CreateProjectConsumerGroup(project, group);
            System.out.printf("create ConsumerGroup %s success\n", group);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private void createLogStore(final String project, final String logStore) {
        LogStore store = new LogStore(logStore, 2, 2);
        try {
            client.CreateLogStore(project, store);
            System.out.printf("create logstore %s.%s success\n", project, logStore);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private void createLink(final String srcProject, final String srcLogStore, final String linkProject, final String linkLogStore) {

        LinkStore link = new LinkStore(linkLogStore, srcProject, srcLogStore);
        try {
            client.CreateLinkStore(linkProject, link);
            System.out.printf("create link %s.%s to %s.%s success\n", linkProject, linkLogStore, srcProject, srcLogStore);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }

    private void createProject(final String project, final String desc) {
        try {
            client.CreateProject(project, desc);
            System.out.printf("create project %s success\n", project);
        } catch (LogException e) {
            System.err.printf("request %s error %s\n", e.GetRequestId(), e.GetErrorMessage());
        }
    }
}
