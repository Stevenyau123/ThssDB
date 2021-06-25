package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import java.util.Iterator;

public class QueryTable implements Iterator<Row> {

       
	
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