package org.apache.hadoop.yarn.util;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class ReduceSize implements Serializable {

    private static final long serialVersionUID = 1L;

    public ArrayList<Long> reduceSizeArray;
    public ArrayList<Integer> reduceTaskIdArray;
    public String ID;
    public int allocatedNum;

    public ReduceSize(ApplicationId applicationId) {
      this.ID = applicationId.getClusterTimestamp() + "_" + applicationId.getId();
      reduceSizeArray = new ArrayList<>();
      reduceTaskIdArray = new ArrayList<>();
      allocatedNum = 0;
    }

    @Override
    public int hashCode() {
      return ID.hashCode();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("id: ").append(ID)
        .append(", reduce task id arry: ").append(reduceTaskIdArray)
        .append(", reduce size array: ").append(reduceSizeArray)
        .append(", allocated num: ").append(allocatedNum);
      return sb.toString();
    }

  }