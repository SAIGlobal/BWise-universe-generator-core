package com.bwise.eUniverse;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CConfig {
	protected static final Logger parentLogger = LogManager.getLogger();
	
	String conffile = "";
	
	
	
	String dbtype ="";
	String CMS_LOG_USER ="";
	String CMS_LOG_PASS ="";
	String CMS_LOG_HOST ="";
	String UniverseName ="";
	String ODBCName = "";
	String ODBCUser = "";
	String ODBCPwd = "";
	
	String TempFolder  ="";
	String DBUser ="";
	String DBPwd  ="";
	String DBServer  ="";
	String DBName  ="";
	String DBPort  ="";
	String OpenUniverse ="";
	String classes  ="";
	//String attributes = cmd.getOptionValue("attributes");
	String dwh  ="";
	String dfs  ="";
	String action ="";
	String controlassessment  ="";
	boolean export = false;
	boolean create = false;
	boolean open = true;

	boolean multilang = false;
	boolean histo = true;

	boolean permission = false;
	
	boolean risktree = false;
	boolean multistep = false;

	boolean portletmode = true;
	int stepsnumber = 3;
	int namelength = 128;
	HashMap<String, String> clones;
	
	CConfig(String[] args) throws IOException {
		
		Options options = new Options();
		options.addOption("conf", true, "conf file");
		options.addOption("type", true, "DB Type : sqlserver or oracle");
		options.addOption("u", true, "CMS_LOG_USER");
		options.addOption("p", true, "CMS_LOG_PASS");
		options.addOption("h", true, "CMS_LOG_HOST");
		options.addOption("n", true, "Name of universe");
		options.addOption("cs", true, "ODBC Source");
		options.addOption("cu", true, "ODBC user");
		options.addOption("cp", true, "ODBC pwd");
		options.addOption("t", true, "Temp Folder");
		options.addOption("du", true, "DB User");
		options.addOption("dp", true, "DB Pwd");
		options.addOption("ds", true, "DB Server");
		options.addOption("dd", true, "DB Name BWise");
		options.addOption("do", true, "DB Port");
		options.addOption("dfs", true, "DFS DB");
		options.addOption("dwh", true, "DWH DB");
		options.addOption("o", true, "Open Existing Universe");
		options.addOption("classes", true, "list classes separated by ;");
		options.addOption("attributes", true, "list attributes separated by ;");
		options.addOption("multilang", false, " Multilanguage enabled?");
		options.addOption("historic", false, " historic enabled?");
		options.addOption("action", true, " action is CREATE or OPEN or EXPORT");
		options.addOption("permission", false, " add permission to table");
		options.addOption("portletmode", true, " Portlet Mode");
		options.addOption("controlassessment", true, " Control Assessment tables");
		options.addOption("risktree", false, " add risk tree table");
		options.addOption("multistep", false, " add multistepassessment tables");
		options.addOption("stepsnumber", true, "Number of steps for OA");
		options.addOption("namelength", true, "max table length name");
		
		parentLogger.log(Level.TRACE,"Parsing inputs");
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		conffile = cmd.getOptionValue("conf");
		
		clones = new HashMap<String,String>();
		
	if (conffile != null && conffile.length()>0) {
		Reader r= new FileReader(conffile);
		
		
		Map<String, Properties> inifile = util.parseINI(r);
		
		for (Map.Entry<String, Properties> entry : inifile.entrySet()) {
	        System.out.println("-----------"+entry.getKey() + "-----------" );
	        
	        
	        
	        entry.getValue().forEach( 
	                (k, v) -> System.out.println( k + " : " + v)); 
	        
	        
	        if(entry.getKey().toLowerCase().equals("general")) {
	        	classes = entry.getValue().get("classes").toString();
	        	multilang = entry.getValue().get("multilang") != null && entry.getValue().get("multilang").toString().toLowerCase().equals("true") ? true:false;
	        	histo = entry.getValue().get("historic") != null &&  entry.getValue().get("historic").toString().toLowerCase().equals("true") ? true:false;
	        	permission = entry.getValue().get("permission") != null && entry.getValue().get("permission").toString().toLowerCase().equals("true") ? true:false;
	        	controlassessment = entry.getValue().get("controlassessment") != null ? entry.getValue().get("controlassessment").toString() : "none";
	        	risktree = entry.getValue().get("risktree") != null && entry.getValue().get("risktree").toString().toLowerCase().equals("true") ? true:false;
	        	multistep = entry.getValue().get("multistep") != null && entry.getValue().get("multistep").toString().toLowerCase().equals("true") ? true:false;
	        	parentLogger.log(Level.INFO,"Value multistep="+ entry.getValue().get("multistep").toString());
	        	action = entry.getValue().get("classes").toString().toUpperCase();
	        	
	        	stepsnumber = entry.getValue().get("stepsnumber") != null ? Integer.parseInt(entry.getValue().get("stepsnumber").toString()) : 3;
	        	namelength = entry.getValue().get("namelength") != null ? Integer.parseInt(entry.getValue().get("namelength").toString()) : 128;
	        	
	        	if (action!= null && action.length() > 0 && action.equals("EXPORT"))
					export = true;
				if (action!= null && action.length() > 0 && action.equals("CREATE"))
					create = true;
				if (action!= null && action.length() > 0 && action.equals("OPEN"))
					open = true;
				
				portletmode = entry.getValue().get("portletmode").toString().toLowerCase().equals("true") ? true:false;
				TempFolder = entry.getValue().get("tempfolder").toString();
			}
	        else  if(entry.getKey().toLowerCase().equals("SQLConnections".toLowerCase())) {
	        	dbtype = entry.getValue().get("db_type").toString();
	        	DBServer = entry.getValue().get("db_server").toString();
	        	DBPort = entry.getValue().get("db_port").toString();
	        	DBName = entry.getValue().get("db_bwise").toString();
	        	DBUser = entry.getValue().get("db_user").toString();
	        	DBPwd = entry.getValue().get("db_pwd").toString();
	        	dwh  = entry.getValue().get("db_dwh").toString();
	        }
	        else  if(entry.getKey().toLowerCase().equals("BOConnections".toLowerCase())) {
	        	CMS_LOG_HOST = entry.getValue().get("cms_host").toString();
	        	CMS_LOG_USER = entry.getValue().get("cms_user").toString();
	        	CMS_LOG_PASS = entry.getValue().get("cms_pwd").toString();
	        	OpenUniverse = entry.getValue().get("universe_path").toString();
	        	UniverseName = entry.getValue().get("universe_name").toString();
	        }  
	        else  if(entry.getKey().toLowerCase().equals("Clones".toLowerCase())) {
	        	
	        	entry.getValue().forEach( 
		                (k, v) -> clones.put(k.toString(), v.toString())
		                
	        			);
	        	
	        }
	        
	        } 
	
		
	        
	        
	    }else
	    {
	    	 dbtype = cmd.getOptionValue("type", "sqlserver");
			 CMS_LOG_USER = cmd.getOptionValue("u");
			 CMS_LOG_PASS = cmd.getOptionValue("p");
			 CMS_LOG_HOST = cmd.getOptionValue("h");
			 UniverseName = cmd.getOptionValue("n");
			 ODBCName = "";
			 ODBCUser = "";
			 ODBCPwd = "";
			if (cmd.hasOption("cs"))
				ODBCName = cmd.getOptionValue("cs");
			if (cmd.hasOption("cu"))
				ODBCUser = cmd.getOptionValue("cu");
			if (cmd.hasOption("cp"))
				ODBCPwd = cmd.getOptionValue("cp");
			 TempFolder = cmd.getOptionValue("t");
			 DBUser = cmd.getOptionValue("du");
			 DBPwd = cmd.getOptionValue("dp");
			 DBServer = cmd.getOptionValue("ds");
			 DBName = cmd.getOptionValue("dd");
			 DBPort = cmd.getOptionValue("do");
			 OpenUniverse = cmd.getOptionValue("o");
			 classes = cmd.getOptionValue("classes");
			//String attributes = cmd.getOptionValue("attributes");
			 dwh = cmd.getOptionValue("dwh");
			 dfs = cmd.getOptionValue("dfs");
			 action = cmd.getOptionValue("action");
			 controlassessment = cmd.getOptionValue("controlassessment","none");
			 export = false;
			 create = false;
			 open = false;
			if (action.equals("EXPORT"))
				export = true;
			if (action.equals("CREATE"))
				create = true;
			if (action.equals("OPEN"))
				open = true;
			 multilang = cmd.hasOption("multilang");
			 histo = cmd.hasOption("historic");

			 permission = cmd.hasOption("permission");
			
			 risktree = cmd.hasOption("risktree");
			 multistep = cmd.hasOption("multistep");

			 portletmode = cmd.getOptionValue("portletmode", "true").equals("true") ? true : false;
	    }
		
	}
}
