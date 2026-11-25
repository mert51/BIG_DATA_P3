package com.sau;

public enum Topic {
    ACCOUNTING(10, "accounting"),
    RESEARCH(20, "research"),
    SALES(30, "sales"),
    OPERATIONS(40, "operations");

    private final int value;
    private final String topicName;

    Topic(int value, String topicName) {
        this.value = value;
        this.topicName = topicName;
    }

    public static String getTopic(int val) {
        for (Topic topic : Topic.values()) {
            if (topic.value == val) {
                return topic.topicName;
            }
        }
        throw new IllegalArgumentException("Unknown department number: " + val);
    }
}
