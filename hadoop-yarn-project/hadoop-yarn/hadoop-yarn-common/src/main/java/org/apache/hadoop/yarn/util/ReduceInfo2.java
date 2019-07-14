package org.apache.hadoop.yarn.util;

import java.io.Serializable;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class ReduceInfo2 implements  Serializable {
    public int reduceSize;
    public String ID;
    public int host_cnt = 10; //because we have 10 machines
    public long total_size;
    public long reduce_info[][];
    public ReduceInfo2(int reduceSize, ApplicationId applicationId){
        this.ID = applicationId.getClusterTimestamp() + "_" + applicationId.getId();
        this.reduceSize = reduceSize;
        reduce_info = new long[host_cnt][reduceSize + 1];
        total_size = 0;
        for(int i=0; i < host_cnt; i++)
            for(int j=0; j < reduceSize+1; j++)
                reduce_info[i][j] = 0;

    }
    public void put(int host, int index, long size){
        reduce_info[host - 1][index] += size;
        total_size += size;
    }
    public long get(int reducer_id){
        long ret = 0;
        for(int i=0 ; i < host_cnt ;i++)
            ret += reduce_info[i][reducer_id];
        return ret;
    }

    public String toString(){
        String ret = ID + ";" + total_size + ";" + reduceSize + ";" + "\n";
        for(int i=0; i < host_cnt; i++){
            ret += "Host-"+i+"\n";
            for(int j=0; j < reduceSize+1; j++){
                ret += j + ":" + reduce_info[i][j]+";";
            }
            ret += "\n";
        }
        return ret;
    }

}
