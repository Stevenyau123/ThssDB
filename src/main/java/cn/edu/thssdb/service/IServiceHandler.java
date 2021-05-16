package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLBaseListener;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.thrift.TException;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  // username: "root", password: ""
  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    ConnectResp resp = new ConnectResp();
    if (req.username.equals(Global.USERNAME) && req.password.equals(Global.PASSWORD)) {
        resp.setSessionId(req.hashCode());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
    }
    else {
        resp.setStatus(new Status(Global.FAILURE_CODE));
    }
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    DisconnetResp resp = new DisconnetResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    ThssDB thssDB = ThssDB.getInstance();
    Manager manager = Manager.getInstance();
    ExecuteStatementResp resp = new ExecuteStatementResp();
    String statement = req.statement;
//    manager.writeLog(statement);
    long sessionId = req.getSessionId();
    CodePointCharStream charStream = CharStreams.fromString(statement);
    SQLLexer lexer = new SQLLexer(charStream);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);
    ParseTree tree = parser.parse();
    ParseTreeWalker walker = new ParseTreeWalker();
    SQLBaseListener listener = new SQLBaseListener();
    walker.walk(listener, tree);
    //resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }
}
