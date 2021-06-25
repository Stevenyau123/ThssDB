package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.CustomIOException;
import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO 等待元数据，初始化Table

  }

  private void recover(ArrayList<Row> rows) {
    for(Row row:rows){
      index.put(row.getEntries().get(primaryIndex), row);
    }
  }

  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), row);
  }

  public void delete(Row row) {
    index.remove(row.getEntries().get(primaryIndex));
  }

  public void update(Row oldRow, Row newRow) {
    if(oldRow.getEntries().get(primaryIndex).compareTo(newRow.getEntries().get(primaryIndex))==0) {
      index.update(newRow.getEntries().get(primaryIndex), newRow);
    }
    else {
      try {
        delete(oldRow);
        insert(newRow);
      }
      catch (DuplicateKeyException e){
        throw e;
      }
    }
  }

  private void serialize() {
    try {
      FileOutputStream fos = new FileOutputStream(Global.STORE_PATH);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fos);
      while (iterator().hasNext()) {
        objectOutputStream.writeObject(iterator().next());
      }
      objectOutputStream.flush();
      objectOutputStream.close();
    } catch (IOException e) {
      throw new CustomIOException();
    }
  }

  private ArrayList<Row> deserialize() {
    try {
      ArrayList<Row> objs = new ArrayList<>();
      FileInputStream fis = new FileInputStream(Global.STORE_PATH);
      ObjectInputStream objectInputStream = new ObjectInputStream(fis);
      while (true) {
        try {
          Row obj = (Row) objectInputStream.readObject();
          objs.add(obj);
        } catch (EOFException e) {
          break;
        } catch (ClassNotFoundException e) {
          objectInputStream.close();
          new File(Global.STORE_PATH).delete();
          //throw new WrongDataException();
        }
      }
      objectInputStream.close();
      return objs;
    } catch (IOException e) {
      new File(Global.STORE_PATH).delete();
      return new ArrayList<>();
    }
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
