package com.aliyun.openservices.log.response;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ProjectConsumerGroupHeartBeatResponse extends Response {

    private static final long serialVersionUID = 3558359690750583972L;
    private Map<String, ArrayList<Integer>> logStoreShards;

    public ProjectConsumerGroupHeartBeatResponse(Map<String, String> headers, JSONObject obj) {
        super(headers);
        logStoreShards = new HashMap<String, ArrayList<Integer>>();

        JSONObject logStoreShardsMap = obj.getJSONObject("logstores");
        Iterator<String> keys = logStoreShardsMap.keys();
        while (keys.hasNext()) {
            String logStore = keys.next();
            JSONArray shardJsonArray = logStoreShardsMap.getJSONArray(logStore);

            ArrayList<Integer> shardList = new ArrayList<Integer>();
            for (int i = 0; i < shardJsonArray.size(); ++i) {
                shardList.add(shardJsonArray.getInt(i));
            }
            this.logStoreShards.put(logStore, shardList);
        }
    }

    /**
     * @return the shards consumer should held in time
     */
    public Map<String, ArrayList<Integer>> getLogStoreShards() {
        return logStoreShards;
    }

    public void setLogStoreShards(Map<String, ArrayList<Integer>> logStoreShards) {
        this.logStoreShards = logStoreShards;
    }
}
