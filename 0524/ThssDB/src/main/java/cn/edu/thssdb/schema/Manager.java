package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


//管理数据库
public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
 Connection conn = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/mydb");

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
  }


//创建数据库
  private void createDatabaseIfNotExists() {
    // TODO database mydb

   String drop_sql = "DROP database IF EXISTS " + databases+ ";";
        Log.d(TAG, "drop_sql:" + drop_sql);
        conn .execSQL(drop_sql);

   String create_sql = "CREATE database IF NOT EXISTS " + databases;

conn .execSQL(create_sql);


  }

  private void deleteDatabase() {
    // TODO
 String drop_sql = "DROP database";
conn .execSQL(create_sql);

  }

  public void switchDatabase() {
    // TODO
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
