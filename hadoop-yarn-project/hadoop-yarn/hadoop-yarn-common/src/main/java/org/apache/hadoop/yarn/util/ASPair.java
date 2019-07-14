package org.apache.hadoop.yarn.util;

import java.io.Serializable;

public class ASPair implements Serializable {
    public String ApplicationID;
    public long size;
    public ASPair(String ApplicationID, long size){
        this.ApplicationID = ApplicationID;
        this.size = size;
    }

    @Override
    public String toString() {
        return ApplicationID;
    }
}
