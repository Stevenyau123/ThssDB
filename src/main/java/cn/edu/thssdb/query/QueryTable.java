package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class QueryTable implements Iterator<Row> {

//  QueryTable() {
//    // TODO
//  }
//
//  @Override
//  public boolean hasNext() {
//    // TODO
//    return true;
//  }
//
//  @Override
//  public Row next() {
//    // TODO
//    return null;
//  }



  public void SetLogicSelect(Logic s) {
    this.mLogicSelect = s;
  }

  @Override
  public JointRow next() {

    this.mQueue = new LinkedList<>();
    this.isFirst = true;
    if (mQueue.isEmpty()) {
      PrepareNext();
      if(isFirst) {
        isFirst = false;
      }
    }
    JointRow result = null;
    if(!mQueue.isEmpty()) {
      result = mQueue.poll();
    }
    else
    {
      return null;
    }
    if (mQueue.isEmpty()) {
      PrepareNext();
    }
    return result;
  }



  @Override
  public boolean hasNext() {
    return isFirst || !mQueue.isEmpty();


  }
  public  LinkedList<JointRow> mQueue;
  public Logic mLogicSelect;
  public boolean isFirst;
  public ArrayList<Column> mColumns;
  public abstract void PrepareNext();
  public abstract ArrayList<MetaInfo> GenerateMetaInfo();
}