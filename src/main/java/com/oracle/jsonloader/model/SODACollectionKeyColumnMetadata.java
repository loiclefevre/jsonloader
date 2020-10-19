package com.oracle.jsonloader.model;

public class SODACollectionKeyColumnMetadata {
    private String name;
    private String assignmentMethod;

    public SODACollectionKeyColumnMetadata() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAssignmentMethod() {
        return assignmentMethod;
    }

    public void setAssignmentMethod(String assignmentMethod) {
        this.assignmentMethod = assignmentMethod;
    }
}
