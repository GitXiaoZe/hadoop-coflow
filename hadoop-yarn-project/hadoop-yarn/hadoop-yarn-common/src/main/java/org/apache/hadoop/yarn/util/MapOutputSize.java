package org.apache.hadoop.yarn.util;

import java.io.Serializable;

public class MapOutputSize implements Serializable{
    public String ID; //job_id
    public int reduce_num;
    public long outputSize[];
    public long total_size;

    public MapOutputSize(int reduce_num){
        ID = null;
        this.reduce_num = reduce_num;
        outputSize = new long[reduce_num];

        for(int i=0; i < reduce_num; i++)
            outputSize[i] = 0;
    }

    public void addItem(int index, long size){
        outputSize[index] += size;
        total_size += size;
    }

    public void addAnother(MapOutputSize another){
        if(this.reduce_num  != another.reduce_num){
            System.err.println("REDUCE NUMBER is different : " + this.reduce_num +" != " + another.reduce_num);
            return;
        }
        for(int i=0; i<this.reduce_num; i++){
            this.outputSize[i] += another.outputSize[i];
        }
        total_size += another.total_size;
    }
    public long getItem(int index){
        return outputSize[index];
    }

    public String toString(){
        String ret = "reduce_num = " + reduce_num + "; total size = " + total_size + "; ";
        for(int i=0; i < reduce_num; i++){
            ret +=  i + " = " + outputSize[i] + ";";
        }
        return ret;
    }
}
