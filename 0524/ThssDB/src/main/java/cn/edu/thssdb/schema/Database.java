package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


//数据库的创建切换删除
public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;
Connection conn = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/mydb");

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // TODO
  }

  public void create(String name, Column[] columns) {
    // TODO database mydb

   String drop_sql = "DROP database IF EXISTS " + databases+ ";";
        Log.d(TAG, "drop_sql:" + drop_sql);
        conn .execSQL(drop_sql);

   String create_sql = "CREATE database IF NOT EXISTS " + databases;

conn .execSQL(create_sql);


  }

  public void drop() {
    // TODO

String drop_sql = "DROP database";
conn .execSQL(create_sql);
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
