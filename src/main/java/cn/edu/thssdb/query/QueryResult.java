package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

//  private List<MetaInfo> metaInfoInfos;
//  private List<Integer> index;
//  private List<Cell> attrs;
//
//  public QueryResult(QueryTable[] queryTables) {
//    // TODO
//    this.index = new ArrayList<>();
//    this.attrs = new ArrayList<>();
//  }
//
//  public static Row combineRow(LinkedList<Row> rows) {
//    // TODO
//    return null;
//  }
//
//  public Row generateQueryRecord(Row row) {
//    // TODO
//    return null;
//  }
  public QueryTable mTable;
  public ArrayList<MetaInfo> mMetaInfoList;
  public boolean mWhetherDistinct;
  public HashSet<String> mHashSet;
  public boolean mWhetherRight;
  public String mErrorMessage;
  public ArrayList<Integer> mColumnIndex;
  public List<String> mColumnName;
  public ArrayList<Row> mResultList;






  private void InitColumns(String[] selectColumns) {
    this.mColumnIndex = new ArrayList<>();
    this.mColumnName = new ArrayList<>();


    if (selectColumns != null) {
      for (String column_name : selectColumns) {
        this.mColumnIndex.add(GetColumnIndex(column_name));
        this.mColumnName.add(column_name);
      }
    }

    else {
      int offset = 0;
      for (MetaInfo metaInfo : mMetaInfoList) {
        for (int i = 0; i < metaInfo.GetColumnSize(); i++) {
          String full_name = metaInfo.GetFullName(i);
          this.mColumnIndex.add(offset + i);
          this.mColumnName.add(full_name);
        }
        offset += metaInfo.GetColumnSize();
      }
    }
  }


  public String MetaToString() {
    String result = "";
    for(int i = 0; i < mColumnName.size(); i ++) {
      result += mColumnName.get(i);
      if(i != mColumnName.size() - 1) {
        result += ", ";
      }
    }
    return result;
  }



  private String[] SplitColumnName(String full_name) {
    String[] splited_name = full_name.split("\\.");
    if (splited_name.length != 2) {
      throw new AttributeInvalidException(full_name);
    }
    return splited_name;
  }





  public void GenerateQueryRecords() {
    while(mTable.hasNext()) {
      JointRow new_row = mTable.next();
      if(new_row == null) {
        break;
      }
      Entry[] entries = new Entry[mColumnIndex.size()];
      ArrayList<Entry> full_entries = new_row.getEntries();
      for(int i = 0; i < mColumnIndex.size(); i ++) {
        int index = mColumnIndex.get(i);
        entries[i] = full_entries.get(index);
      }
      Row the_row = new Row(entries);
      String row_string = the_row.toString();
      if(!mWhetherDistinct || !mHashSet.contains(row_string)) {
        mResultList.add(the_row);
        if(mWhetherDistinct) {
          mHashSet.add(row_string);
        }
      }
    }
  }




  public QueryResult(QueryTable queryTable, String[] selectColumns, boolean whetherDistinct) {
    this.mTable = queryTable;
    this.mWhetherDistinct = whetherDistinct;
    this.mHashSet = new HashSet<>();
    mWhetherRight = true;
    mErrorMessage = "";
    this.mMetaInfoList = new ArrayList<MetaInfo>();
    this.mMetaInfoList.addAll(queryTable.GenerateMetaInfo());
    this.mResultList = new ArrayList<Row>();
    InitColumns(selectColumns);
  }


  public int GetColumnIndex(String name) {
    int index = 0;


    if (!name.contains(".")) {
      int equal_sum = 0;
      int total_index = 0;
      for (int i = 0; i < mMetaInfoList.size(); i++) {
        int current_index = mMetaInfoList.get(i).ColumnFind(name);
        if(current_index >= 0) {
          equal_sum ++;
          index = current_index + total_index;
        }
        total_index += mMetaInfoList.get(i).GetColumnSize();
      }
      if (equal_sum < 1) {
        throw new AttributeNotFoundException(name);
      }
      else if (equal_sum > 1) {
        throw new AttributeCollisionException(name);
      }
    }

    else {
      String[] splited_names = SplitColumnName(name);
      String table_name = splited_names[0];
      String entry_name = splited_names[1];
      boolean whether_find = false;
      int total_index = 0;
      for (int i = 0; i < mMetaInfoList.size(); i++) {
        String current_name = mMetaInfoList.get(i).GetTableName();
        if (!current_name.equals(table_name)) {
          total_index += mMetaInfoList.get(i).GetColumnSize();
          continue;
        }

        int current_index = mMetaInfoList.get(i).ColumnFind(entry_name);
        if (current_index >= 0) {
          whether_find = true;
          index = current_index + total_index;
          break;
        }
      }

    }
    return index;
  }
}
