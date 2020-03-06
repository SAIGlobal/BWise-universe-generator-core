package com.bwise.eUniverse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import oracle.jdbc.pool.OracleDataSource;

public class BWiseDM {
	
	protected static final Logger parentLogger = LogManager.getLogger();
	
	SQLServerDataSource dss;
	OracleDataSource dso;
	String dfs;
	String dwh;
	String dbtype;
	DataSource ds;
	public BWiseDM (String User, String Pwd, String Server, String Port, String DB, String pdfs, String pdwh, String pdbtype) {
		
		dbtype = pdbtype;
		
		
		
		if (dbtype.equals("oracle"))
			try {
				dso = new OracleDataSource();
				
		
				dso.setUser(User);
				dso.setPassword(Pwd);
				dso.setServerName(Server);
				dso.setPortNumber(Integer.parseInt(Port));
				dso.setServiceName(DB);

		//dso.setURL("jdbc:oracle:thin:@"+Server+":"+Integer.parseInt(Port)+":"+DB);
	
				dso.setDriverType("thin");
				
				ds = dso;
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else {
			dss = new SQLServerDataSource();
			
			// Create datasource.
			dss = new SQLServerDataSource();
			dss.setUser(User);
			dss.setPassword(Pwd);
			dss.setServerName(Server);
			dss.setPortNumber(Integer.parseInt(Port));
			dss.setDatabaseName(DB);
		      
		      ds = dss;
			
		}
		
		
	
	      dfs = pdfs;
	      dwh =pdwh;
	  
	      
	}
	public  List<Map<String, String>>  runSQL(String SQL) {
	
	      

	      
	      try (Connection con = ds.getConnection();
	    		  Statement stmt = con.createStatement();) {
	    	  stmt.setFetchSize(100);
				con.setAutoCommit(false);
	          ResultSet rs = stmt.executeQuery(SQL);

	          final List<Map<String, String>> rowList = new ArrayList<Map<String, String>>();
	          final ResultSetMetaData meta = rs.getMetaData();
	          final int columnCount = meta.getColumnCount();
	          
	          // Iterate through the data in the result set and display it.
	         while (rs.next()) {
	        	 Map<String, String> columnList = new HashMap<>();
	        	
	        	 
	        	 for (int column = 1; column <= columnCount; ++column) 
	        	    {
	        	        final Object value = rs.getObject(column);

	        	        columnList.put(meta.getColumnName(column), String.valueOf(value));
	        	    }
	        	 rowList.add(columnList);
	          }
	          
	          return rowList;
	      }
	      // Handle any errors that may have occurred.
	      catch (SQLException e) {
	    	  parentLogger.log(Level.ERROR,"SQL = " + SQL);
	          e.printStackTrace();
	          return null;
	      }
	}

	public  List<Map<String, String>>  getDM(List<String>   classes) {
		
		String listclasses = String.join("','",classes);
		

		parentLogger.log(Level.INFO,"classes = " + listclasses);
		return runSQL("SELECT t.* "
				+ ",(select distinct TABLENAME from V_DM_TABLES x where x.CLASSNAME = t.CLASSNAME and x.TABLETYPE = 'DimensionTable') PARENT "
				+ "FROM V_DM_TABLES t "
				+ "WHERE t.CLASSNAME in('"+listclasses+"') "
						+ " ORDER BY CLASSNAME,SEQUENCENUMBER");
	
	}
	
	public  List<Map<String, String>>  getDM(String   classes) {
		

		

		parentLogger.log(Level.INFO,"classes = " + classes);
		return runSQL("SELECT t.* ,(select distinct TABLENAME from V_DM_TABLES x where x.CLASSNAME = t.CLASSNAME and x.TABLETYPE = 'DimensionTable') PARENT \r\n" + 
				"				FROM V_DM_TABLES t \r\n" + 
				"				WHERE t.CLASSNAME = '"+classes+"'\r\n" + 
				//"                and t.tabletype = 'DimensionTable'\r\n" + 
				"					 ORDER BY CLASSNAME,SEQUENCENUMBER");
	
	}
	
	public boolean  isHistoricDWH() {
		
		String histosql = "";
		if (dbtype.equals("sqlserver"))
			histosql = "select CASE WHEN max(BWPROPVALUE) IS NULL THEN 'true' ELSE max(BWPROPVALUE)  END VALUE\r\n" + 
					"				from "+dwh+".dbo.A_SYSTEMPROPERTIES \r\n" + 
					"				where BWPROPNAME = 'DwhHistoryEnabled'";
		else if (dbtype.equals("oracle"))
			histosql = "select CASE WHEN max(BWPROPVALUE) IS NULL THEN 'true' ELSE max(BWPROPVALUE)  END VALUE\r\n" + 
					"				from "+dwh+".A_SYSTEMPROPERTIES \r\n" + 
					"				where BWPROPNAME = 'DwhHistoryEnabled'";
		
		List<Map<String, String>> res =  runSQL(histosql);
		if (res.get(0).get("VALUE").equals("false"))
			return false;
		else 
			return true;
	
	}
	
	
	
	
	public  List<Map<String, String>>  getColumns(String Table, String attributes, String bwiseclass) {
		
		String sql = "select T.*,   E.NAME ROOTCLASSLINK " + 
				"from V_DM_COLUMNS T  " +
				"left join M_CLASSDEF D on D.NAME = t.LINKCLASSNAME " + 
				"left join M_CLASSDEF E on D.SUPERCLASS = E.CLASSDEFID  "
				+ "left join (SELECT ROWNUM MYORDER,layout_id CLASSNAME,trim(COLUMN_VALUE) p\r\n" + 
				"      FROM (SELECT layout_id,form_fields str FROM R_FORM_LAYOUT  where layout_id= '"+bwiseclass+"') DATA, xmltable(('\"' || REPLACE(str, '|', '\",\"') || '\"'))) o on o.classname = t.classname and o.p = t.attribute "+

    	  		"where  t.columntype = 'VALUE' and t.TABLENAME = '"+Table+"'"
    	  				+ " ORDER BY t.CLASSNAME ,NVL(o.MYORDER, 0), t.SEQUENCENUMBER";
		
		if (attributes != null && attributes.length() > 0) {
			attributes = String.join("','",attributes.trim().split(";"));
			
			sql += " AND ATTRIBUTE IN ('"+attributes+"')";
		}
		parentLogger.log(Level.TRACE,"SQL getColumns =" +sql);
		
		return runSQL(sql);
		
	}
	public String getRiskTreeSQL(String WhereClause) {
		String SQLRisktree = "SELECT T.* " + System.lineSeparator() + "FROM A_RISKTREE T "
				+ System.lineSeparator()
				+ "WHERE " + WhereClause;
		return SQLRisktree;
	}
	public String getIssueWrapperSQL(CTable t, String WhereClause) {
		
		String SQLWrapper = "SELECT\r\n" + "T." + t.getPrimaryKey() + " ID\r\n"
				+ ",T.RELATEDNAME ENTITIESNAME\r\n" + ",T.RELATEDID ENTITIESID\r\n"
				+ ",T.RELATEDBWID ENTITIESBWID\r\n" + ",X.RELATEDNAME GROUPNAME\r\n"
				+ ",X.RELATEDID GROUPID\r\n" + ",X.RELATEDBWID GROUPBWID\r\n" + "FROM X_"
				+ t.getNameRaw() + "_ENTITIES T\r\n" + "\r\n" + "LEFT JOIN X_" + t.getNameRaw()
				+ "_DIVISIONS X\r\n" + "ON T." + t.getPrimaryKey() + " = X." + t.getPrimaryKey() + " AND "
				+ WhereClause.replace("T.", "X.") + "\r\n" + "WHERE " + WhereClause;
		
		return SQLWrapper;
	}
	public String getCategoriesSQL(String SQLNTable, String WhereClause ) {
		String SQLNGroup = 	"SELECT\r\n" + 
				"T.CHILDID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=0 THEN T.PARENTID END) AS LEVEL1ID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=0 THEN T.PARENTNAME END) AS LEVEL1NAME,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=1 THEN T.PARENTID END) AS LEVEL2ID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=1 THEN T.PARENTNAME END) AS LEVEL2NAME,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=2 THEN T.PARENTID END) AS LEVEL3ID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=2 THEN T.PARENTNAME END) AS LEVEL3NAME,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=3 THEN T.PARENTID END) AS LEVEL4ID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=3 THEN T.PARENTNAME END) AS LEVEL4NAME,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=4 THEN T.PARENTID END) AS LEVEL5ID,\r\n" + 
				"MAX(CASE WHEN T.PARENTLEVEL=4 THEN T.PARENTNAME END) AS LEVEL5NAME\r\n" + 
				"FROM "+SQLNTable+" T\r\n" + 
				"WHERE " + WhereClause +
				" GROUP BY T.CHILDID";
		
		return SQLNGroup;
	}
	public String getWorkflowSQL() {
		String SignoffSQL = "";
		if (dbtype.equals("sqlserver"))
			SignoffSQL = "SELECT \r\n" + "  W.PROPOSALDWHID\r\n" + "  ,ST.*\r\n"
				+ "  ,CONVERT(date,SUBSTRING(CAST(ST.VALIDFROM AS varchar),2,6),112) AS STEP_DATE\r\n"
				+ "FROM A_WORKFLOWSTEP ST\r\n" + "\r\n" + "INNER JOIN A_WORKFLOW W\r\n"
				+ "ON ST.WORKFLOWID = W.WORKFLOWID\r\n" + "\r\n" + "WHERE ST.ISLIVE = 1";
		
		else if (dbtype.equals("oracle")) 
			SignoffSQL = "SELECT  W.PROPOSALDWHID ,ST.*\r\n" + 
					", TO_DATE(SUBSTR(ST.VALIDFROM, 2, 12),'YYMMDDHH24MISS') STEP_DATE\r\n" + 
					"				FROM A_WORKFLOWSTEP ST INNER JOIN A_WORKFLOW W\r\n" + 
					"				ON ST.WORKFLOWID = W.WORKFLOWID WHERE ST.ISLIVE = 1";
		
		return SignoffSQL;
	}
	public   Map<String, String> getTablesfromDWH() {
		
		String sql = "";
		
		if (dbtype.equals("sqlserver"))
			sql = "select * from (\r\n" + 
				"select  \r\n" + 
				"        t.[name]           TABLENAME,\r\n" + 
				"        c.[name]           COLUMNNAME,\r\n" + 
				"		ROW_NUMBER() over (partition by t.[name] order by c.column_id) MYORDER\r\n" + 
				"from     "+dwh+".sys.tables  t\r\n" + 
				"inner join   "+dwh+".sys.columns c\r\n" + 
				"on t.object_id = c.object_id\r\n" + 
				"\r\n" + 
				//"where  t.[name]  like 'T_%' or t.[name]  like 'X_%' or t.[name]  like 'L_%'  or t.[name]  like 'P_%'\r\n" + 
				") M\r\n" + 
				"where MYORDER = 1"
				+ " order by 1";
		
		else if (dbtype.equals("oracle"))
			/*sql="select TABLE_NAME TABLENAME,column_name COLUMNNAME " + 
					"from all_tab_columns t " + 
					"where OWNER ='"+dwh.toUpperCase()+"' " + 
					"and (  t.TABLE_NAME  like 'T_%' or t.TABLE_NAME  like 'X_%' or t.TABLE_NAME  like 'L_%'  )  " + 
					"and COLUMN_ID = 1 order by 1";*/
		sql="SELECT * FROM (select t.TABLE_NAME TABLENAME,column_name COLUMNNAME,COLUMN_ID\r\n" + 
				"                    from all_tab_columns t\r\n" + 
				"                     where OWNER='"+dwh.toUpperCase()+"') A\r\n" + 
				"                    where  COLUMN_ID = 1 \r\n" 
				//"                    and (  a.TABLENAME  like 'T_%' or a.TABLENAME  like 'X_%' or a.TABLENAME  like 'L_%'  ) "
				+ "order by 1";
		
		try (Connection con = ds.getConnection();
				
				
	    		  ) {
			
			
			String tempSchema = con.getSchema();
			con.setSchema(dwh.toUpperCase());
			//con.setAutoCommit(false);

			
			Statement stmt = con.createStatement();
			con.setSchema(dwh.toUpperCase());
			stmt.setFetchSize(200);
			parentLogger.log(Level.DEBUG,"getting tables...");



			
	          ResultSet rs = stmt.executeQuery(sql);

	          parentLogger.log(Level.DEBUG,"fetching tables...");
	         
	          Map<String, String> tablelist = new HashMap<>();
	          // Iterate through the data in the result set and display it.
	         while (rs.next()) {
	        	 
	        	
	        	 tablelist.put(rs.getObject(1).toString(),rs.getObject(2).toString());


	        	 parentLogger.log(Level.DEBUG,"adding " + rs.getObject(1).toString() + " with pk " + rs.getObject(2).toString());
	          }
	         
	         con.setSchema(tempSchema);
	          return tablelist;

	      }
	      // Handle any errors that may have occurred.
	      catch (SQLException e) {
	          e.printStackTrace();
	          return null;
	      }
		
	}
	
	
	public  List<Map<String, String>>  getEnumList(String TableName) {
		
		String sql = "select distinct  c.ATTRIBUTE,c.COLUMNNAME, t.CLASSNAME, t.TABLENAME, t.PKCOLUMN, t.TABLENAMERAW,t.ISHISTORIC, c.LABEL \r\n" + 
				"from V_DM_COLUMNS C\r\n" + 
				"inner join V_DM_TABLES t on t.CLASSNAME = c.LINKCLASSNAME and t.ROOTCLASS = 'EnumLiteral'\r\n" + 
				"where C.TABLENAME = '"+TableName+"'\r\n" + 
				"and C.type = 'FOREIGN_KEY'";
		
		
		return runSQL(sql);
		
	}
	
	
	public  List<Map<String, String>>  getQuestion(String TableName) {
		
		String sql = "";
		if (dbtype.equals("sqlserver"))
		sql="select q.*\r\n" + 
				"from "+dwh+".dbo."+TableName+" t\r\n" + 
				"inner join "+dwh+".T_QUESTION q on q.islive=1 and q.questionid = t.relatedid\r\n" + 
				"where t.islive=1\r\n" + 
				"and q.ishidden=0\r\n" + 
				"and q.typename not in( 'No Answer','Create Issue')\r\n" + 
				"and q.hasreportingcolumn = 1\r\n" + 
				"order by t.listindex";
		else if (dbtype.equals("oracle"))
			sql="select q.*\r\n" + 
					"from "+dwh+"."+TableName+" t\r\n" + 
					"inner join "+dwh+".T_QUESTION q on q.islive=1 and q.questionid = t.relatedid\r\n" + 
					"where t.islive=1\r\n" + 
					"and q.ishidden=0\r\n" + 
					"and q.typename not in( 'No Answer','Create Issue')\r\n" + 
					"and q.hasreportingcolumn = 1\r\n" + 
					"order by t.listindex";
		
		
		return runSQL(sql);
		
	}
	public  List<Map<String, String>>  getColumnsML(String Table, String attributes) {
		
		String sql = "select T.* \r\n" + 
				", v.TABLENAME TABLELANG from V_DM_COLUMNS T \r\n"
				+ "left join V_DM_TABLES v on v.CLASSNAME = t.linkclassname and v.TABLETYPE = 'DimensionTable' and t.CLASSNAME <> v.classname \r\n" + 
				"and t.linkclassname not in ('Resource','Role','TreeRoot','Document')"
				+ "" + 
    	  		"where  t.columntype = 'VALUE' and t.TABLENAME = '"+Table+"'"
    	  				+ " ORDER BY t.CLASSNAME, t.SEQUENCENUMBER";
		
		if (attributes != null && attributes.length() > 0) {
			attributes = String.join("','",attributes.trim().split(";"));
			
			sql += " AND ATTRIBUTE IN ('"+attributes+"')";
		}
		return runSQL(sql);
		
	}
	
	public void insert(List<ArrayList<String>> array, String UniverseName) throws SQLException {
		
		String deletesql = "";
		if (dbtype.equals("sqlserver"))
			deletesql = "DELETE FROM "+dfs+".dbo.T_UNIVERSE WHERE UNIVERSENAME='"+UniverseName+"'";
		else if (dbtype.equals("oracle"))
			deletesql = "DELETE FROM "+dfs+".T_UNIVERSE WHERE UNIVERSENAME='"+UniverseName+"'";
		
		
		
		try (Connection con = ds.getConnection();
			     Statement statement = con.createStatement()) {
			  statement.executeUpdate(deletesql);
			  try {
				con.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			}
		
		final int batchSize = 1000;
		int count = 0;


		parentLogger.log(Level.DEBUG,array.size() + " to insert");
		
		String insertsql = "";
		if (dbtype.equals("sqlserver"))
			insertsql = "insert into "+dfs+".dbo.T_UNIVERSE (CLASSNAME, ATTRIBUTE, UNIVERSENAME) values (?, ?, ?)";
		else if (dbtype.equals("oracle"))
			insertsql = "insert into "+dfs+".T_UNIVERSE (CLASSNAME, ATTRIBUTE, UNIVERSENAME) values (?, ?, ?)";
		

		try (Connection con = ds.getConnection();
				PreparedStatement ps = con.prepareStatement(insertsql);) {
			
			for(ArrayList<String> a : array) {
				ps.setString(1, a.get(0));


				parentLogger.log(Level.DEBUG," value = " + a.get(1));
				ps.setString(2, a.get(1));
				
				ps.setString(3, UniverseName);

				ps.addBatch();
				
				if(++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			
			
			
			ps.executeBatch(); // insert remaining records
			ps.close();
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
