package com.aliyun.openservices.log.response;

import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ProjectConsumerGroupCheckPointResponse extends Response {
    private static final long serialVersionUID = -5446935677776563121L;
    private Map<String, ArrayList<ConsumerGroupShardCheckPoint>> checkPoints;

    public ProjectConsumerGroupCheckPointResponse(Map<String, String> headers, JSONObject checkPointsMap) {
        super(headers);
        checkPoints = new HashMap<String, ArrayList<ConsumerGroupShardCheckPoint>>();

        Iterator<String> keys = checkPointsMap.keys();
        while (keys.hasNext()) {
            String logStore = keys.next();
            ArrayList<ConsumerGroupShardCheckPoint> cpList = new ArrayList<ConsumerGroupShardCheckPoint>();
            JSONArray cpJsonArray = checkPointsMap.getJSONArray(logStore);

            for (int i = 0; i < cpJsonArray.size(); ++i) {
                ConsumerGroupShardCheckPoint cp = new ConsumerGroupShardCheckPoint();
                cp.Deserialize(cpJsonArray.getJSONObject(i));
                cpList.add(cp);
            }
            checkPoints.put(logStore, cpList);
        }
    }

    public Map<String, ArrayList<ConsumerGroupShardCheckPoint>> getCheckPoints() {
        return checkPoints;
    }

    public void setCheckPoints(Map<String, ArrayList<ConsumerGroupShardCheckPoint>> checkPoints) {
        this.checkPoints = checkPoints;
    }
}
