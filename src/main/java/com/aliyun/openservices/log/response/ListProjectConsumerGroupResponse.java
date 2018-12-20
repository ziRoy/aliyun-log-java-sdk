package com.aliyun.openservices.log.response;

import com.aliyun.openservices.log.common.ProjectConsumerGroup;

import java.util.ArrayList;
import java.util.Map;

public class ListProjectConsumerGroupResponse extends Response {
    private static final long serialVersionUID = -5449137454886127253L;

    private ArrayList<ProjectConsumerGroup> consumerGroups = new ArrayList<ProjectConsumerGroup>();

    public ListProjectConsumerGroupResponse(Map<String, String> headers) {
        super(headers);
    }

    public ArrayList<ProjectConsumerGroup> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(ArrayList<ProjectConsumerGroup> consumerGroups) {
        this.consumerGroups = consumerGroups;
    }
}
