package com.bwise.eUniverse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CClass {
	
	protected static final Logger parentLogger = LogManager.getLogger();
	private String name;
	private String rootClass;
	private String bwiseclassOriginal;
	private boolean isMultilang;
	private boolean hasPermissions;
	private boolean isFramework;
	private boolean isIssue;
	private boolean isInDWH;
	private CTable maintable;
	private List<CDimension> dimensions;
	private List<CTable> crosstables;
	private List<CTable> questiontables;
	private List<CJoin> joins;
	private CTable langtable;
	private List<Map<String, String>> list;
	private String mainname;
	CClass (String pname) {

		name = pname;
		mainname = pname;
		bwiseclassOriginal = pname;
		this.fillMain();
		
	}
	
	CClass (String pname, String pmainname) {

		name = pname;
		mainname = pmainname;
		bwiseclassOriginal = pname;
		this.fillMain();
		
	}
	
	public void setBWiseClassOriginal(String pclass) {
		bwiseclassOriginal = pclass;
	}
	public String getRootClass() {
		return rootClass;
	}
	public CTable getMainTable() {
		return maintable;
	}
	public void setMainTable(CTable t) {
		maintable = t;
	}
	
	public List<CTable> getCrossTable() {
		return crosstables;
	}
	
	public List<CTable> getQuestionTables() {
		return questiontables;
	}
	
	public List<CJoin> getJoins() {
		return joins;
	}
	public void addJoin(CJoin join) {
		this.joins.add(join);
	}
	public void fillMain() {
		dimensions = new ArrayList<CDimension>();
		crosstables = new ArrayList<CTable>();
		joins= new ArrayList<CJoin>();
		questiontables  = new ArrayList<CTable>();
		
		list=  Main.dm.getDM(name);
		
		for (Map<String, String> element : list) {
			if(element.get("TABLETYPE").equals("DimensionTable")) {
				
				this.rootClass = element.get("ROOTCLASS");
				
				CTable t = new CTable("DT_"+ element.get("TABLENAMERAW").toUpperCase(),element.get("TABLENAME"), this.name);
				t.setNameRaw(element.get("TABLENAMERAW"));
				if(!this.mainname.equals(name)) {
					t.setName(mainname);
				}
				t.setAsDimension();
				t.addToView("BWise");
				
				if(Main.config.histo && element.get("ISHISTORIC").equals("1"))
					t.enableHistoric();
				//t.setSQLQuery(t.getDefaultSQL());
				parentLogger.log(Level.INFO,"adding main table " + t.getName());
				maintable = t;
				
				if (Main.config.risktree && this.getFKRiskTree().length() > 0) {
					
				CJoin mainjoin = new CJoin(t.getName(),t.getPrimaryKey(), "DT_RISKTREE", this.getFKRiskTree());
				parentLogger.log(Level.INFO,"adding join to RiskTree from main table " + mainjoin.getSQLJoin());
				this.joins.add(mainjoin);
				}
			}
		}
	}
	public void fillIt() {

		
		

		for (Map<String, String> element : list) {
			 if(element.get("TABLETYPE").equals("CrossTable")) {
				
				 String xname = this.getMainTable().getName() + "_" + element.get("TABLENAMERAW").substring(element.get("TABLENAMERAW").indexOf("_")+1);
				 
				 String class2pass= this.name;
				 //if (this.maintable.isSQLView())
					// class2pass = this.getMainTable().getName() + "_" + element.get("TABLENAMERAW").substring(element.get("TABLENAMERAW").indexOf("_")+1);
				CTable t = new CTable(xname +"_SV",element.get("TABLENAME"),class2pass);
				t.setNameRaw(element.get("TABLENAMERAW"));
				t.setBWiseClass(this.maintable.getBWiseClass());
				t.setAsCrossTable();

				if(Main.config.histo && element.get("ISHISTORIC").equals("1"))
					t.enableHistoric();
				
				//t.setSQLQuery(t.getDefaultSQL());
				
				if(!crosstables.contains(t))  {
					parentLogger.log(Level.INFO,"adding SV table " + t.getName());
					crosstables.add(t);
				}
				
				CJoin svjoin = new CJoin(t.getName(), t.getPrimaryKey(),maintable.getName(),maintable.getPrimaryKey());
				
				this.joins.add(svjoin);
				
				CTable tmv = new CTable(xname +"_MV",element.get("TABLENAME"), this.name);
				tmv.setBWiseClass(this.maintable.getBWiseClass());
				tmv.setNameRaw(element.get("TABLENAMERAW")+"_MV");

				if(Main.config.histo && element.get("ISHISTORIC").equals("1"))
					tmv.enableHistoric();
				
				String WhereClauseMVC1 = "1=1";
				String WhereClauseMVC2 = "1=1";

				if (Main.isHistoricDWH && Main.config.histo) {
					if (Main.config.dbtype.equals("sqlserver")) {
						WhereClauseMVC1 = "((CONVERT(VARCHAR(8), CAST(@Prompt(SD) AS DATE), 112)-19000000)*1000000000000+235959000000) BETWEEN C1.VALIDFROM AND C1.VALIDTO";
						WhereClauseMVC2 = "((CONVERT(VARCHAR(8), CAST(@Prompt(SD) AS DATE), 112)-19000000)*1000000000000+235959000000) BETWEEN C2.VALIDFROM AND C2.VALIDTO";
					} else if (Main.config.dbtype.equals("oracle")) {
						WhereClauseMVC1 = "'1' || TO_CHAR(TO_DATE(@Prompt(SD),'DD-MM-RRRR hh24:mi:ss')+ 1  - INTERVAL '1' SECOND,'YYMMDDHH24MISS') || '000000' BETWEEN C1.VALIDFROM AND C1.VALIDTO";
						WhereClauseMVC2 = "'1' || TO_CHAR(TO_DATE(@Prompt(SD),'DD-MM-RRRR hh24:mi:ss')+ 1  - INTERVAL '1' SECOND,'YYMMDDHH24MISS') || '000000' BETWEEN C2.VALIDFROM AND C2.VALIDTO";
					}

				} else if (Main.isHistoricDWH) {
					WhereClauseMVC1 = "C1.ISLIVE=1";
					WhereClauseMVC2 = "C2.ISLIVE=1";
				}

				String SQLMV = "";
				if (Main.config.dbtype.equals("sqlserver")) {
					SQLMV = "SELECT C1." + tmv.getPrimaryKey() + " ID," + System.lineSeparator() + "("
							+ System.lineSeparator() + "  SELECT" + System.lineSeparator()
							+ "    C2.RELATEDNAME + CHAR(10)" + System.lineSeparator() + "  FROM " + tmv.getSQLTable()
							+ " C2" + System.lineSeparator() + "  WHERE C1." + tmv.getPrimaryKey() + " = C2." + tmv.getPrimaryKey()
							+ System.lineSeparator() + "AND " + WhereClauseMVC2 + System.lineSeparator()
							+ "  ORDER BY RELATEDNAME" + System.lineSeparator() + "  FOR XML PATH('')"
							+ System.lineSeparator() + ") MV" + System.lineSeparator() +

							"FROM " + tmv.getSQLTable() + " C1" + System.lineSeparator() + "WHERE " + WhereClauseMVC1
							+ System.lineSeparator() + "GROUP BY C1." + tmv.getPrimaryKey();
				}
				// TO BE FIXED FOR ORACLE
				else if (Main.config.dbtype.equals("oracle")) {
					SQLMV = "SELECT "+tmv.getPrimaryKey()+" ID,LISTAGG( RELATEDNAME, ', ') WITHIN GROUP (  ORDER BY RELATEDNAME) MV\r\n" + System.lineSeparator() +
							"FROM "+tmv.getSQLTable()+" T\r\n" + System.lineSeparator() +
							"where "+tmv.getWhereClause()+"\r\n" + System.lineSeparator() +
							"group by " + tmv.getPrimaryKey();
				}
				
				

				
				tmv.setSQLQuery(SQLMV);
				tmv.setPrimaryKey("ID");
				tmv.setAsMultiValue();
				if(!crosstables.contains(tmv)) {
					parentLogger.log(Level.INFO,"adding MV table " + t.getName());
					crosstables.add(tmv);
				}
				
				CJoin mvjoin = new CJoin( tmv.getName(), tmv.getPrimaryKey(),maintable.getName(),maintable.getPrimaryKey());
				
				this.joins.add(mvjoin);
			}	
		}
	}
	
	public String getFKRiskTree() {
		
			String SQLRiskTreeJoin = "";
			switch(this.bwiseclassOriginal) {
			  case "Risk":
				  SQLRiskTreeJoin = "RISKID";
				  

			    break;
			  case "ControlMeasure":
				  SQLRiskTreeJoin = "CONTROLID";
			    break;
			    
			  case "ProcessHierarchy":
				  SQLRiskTreeJoin = "PROCHIERID";
			    break;
			  case "SignificantAccount":
				  SQLRiskTreeJoin = "SIGACCID";
			    break;
			  case "InformationSystem":
				  SQLRiskTreeJoin = "INFOSYSID";
			    break;
			  case "BusinessProcess":
				  SQLRiskTreeJoin = "BUSPROCID";
				   break;
			  case "Assertion":
				  SQLRiskTreeJoin = "CTRLOBJID";
				   break;

			  default:
					switch(this.rootClass) {
					  case "AbstractGroup":
						  SQLRiskTreeJoin = "GROUPID";
					    break;
					  case "AbstractInformationComponent":
						  SQLRiskTreeJoin = "INFOCOMPID";
						   break;
					  default:
					    // code block
					}
			}
		return SQLRiskTreeJoin;
	}
	public CTable buildWrapperTable() {
		if(this.rootClass.equals("Issue")) {
CTable t = new CTable("","A_WORKFLOW", this.name + "_N");
		
		t.setSQLQuery(Main.dm.getIssueWrapperSQL(this.getMainTable(), this.getMainTable().getWhereClause()));
		t.setName(this.getMainTable().getName()+"_WRAPPER");
		t.setAsDimension();
		t.setPrimaryKey("ID");
		
		return t;
		}
		return null;
	}
	public CTable buildNTable() {
		String SQLNTable ="";
		String JoinN = this.maintable.getPrimaryKey();
		
		String classname = this.bwiseclassOriginal;
		
		if(this.rootClass.equals("AbstractGroup")) {
			SQLNTable = "N_GROUP";
			JoinN = "PARENTOBJECTID";
		}
		if(this.rootClass.equals("Issue")) {
			SQLNTable = "N_ISSUECATEGORY";
		}
		switch(classname) {
		  case "Risk":
			  SQLNTable = "N_RISKCATEGORY";
			  JoinN = "PARENTOBJECTID";
		    break;
		  case "ControlMeasure":
			  SQLNTable = "N_CONTROLMEASURECATEGORY";
			  JoinN = "PARENTOBJECTID";
		    break;
		    
		  case "SignificantAccount":
			  SQLNTable = "N_SIGNIFICANTACCOUNTCATEGORY";
			  JoinN = "PARENTOBJECTID";
		    break;
		}
		
		if(SQLNTable.length() <= 0)
			return null;
		CTable t = new CTable(SQLNTable,"A_WORKFLOW", this.name + "_N");
		
		t.setSQLQuery(Main.dm.getCategoriesSQL(SQLNTable, this.getMainTable().getWhereClause()));
		t.setName(this.getMainTable().getName()+"_N");
		t.setAsDimension();
		t.setPrimaryKey("CHILDID");
		
		return t;
	}
	
	
	
	public List<CDimension> getCAQuestions(String pfolder) {
		List<CDimension> dimensions = new ArrayList<CDimension>();
		
		String xtablequestion = "";
		String xanswertable = "";
		switch (this.name) {
		case "BWCntrlMonitorSession":
			xtablequestion = "x_controlmonitoringdfntn_qstns";
			xanswertable = "SN_"+ Main.config.controlassessment.toUpperCase();
			break;
		case "BWCntrlReviewSession":
			xtablequestion = "X_CONTROLREVIEWDEFINTN_QUSTNS";
			xanswertable = "SW_"+ Main.config.controlassessment.toUpperCase();
			break;
		case "BWCntrlTestSession":
			xtablequestion = "X_CONTROLTESTDEFINITN_QUESTNS";
			xanswertable = "SU_"+ Main.config.controlassessment.toUpperCase();
			break;
		
		}
		
		if(xtablequestion.equals(""))
			return dimensions;
		
		
		
		String sqlquestionmain = this.getMainTable().getName() +".S_SUBJECTNAME";
		CDimension dimsubject = new CDimension( "Subject Name", this.getMainTable().getName() + "_subjectname", sqlquestionmain, "STRING", pfolder);
		dimsubject.setFolder(pfolder);
		dimensions.add(dimsubject);
		
		sqlquestionmain = this.getMainTable().getName() +".S_SUBJECTTYPE";
		CDimension dimsubjecttype = new CDimension( "Subject Type", this.getMainTable().getName() + "_subjecttype", sqlquestionmain, "STRING", pfolder);
		dimsubjecttype.setFolder(pfolder);
		dimensions.add(dimsubjecttype);
		
		sqlquestionmain = this.getMainTable().getName() +".S_ANSWEREDBY";
		CDimension dimansweredby = new CDimension( "Answered By", this.getMainTable().getName() + "_answeredby", sqlquestionmain, "STRING", pfolder);
		dimansweredby.setFolder(pfolder);
		dimensions.add(dimansweredby);
		
		sqlquestionmain = this.getMainTable().getName() +".S_BWANSWEREDBYNAME";
		CDimension dimansweredbyname = new CDimension( "Answered By Name", this.getMainTable().getName() + "_answeredbyname", sqlquestionmain, "STRING", pfolder);
		dimansweredbyname.setFolder(pfolder);
		dimensions.add(dimansweredbyname);
		
		sqlquestionmain = this.getMainTable().getName() +".S_ANSWERDATE";
		CDimension dimanswerdate = new CDimension( "Answered Date", this.getMainTable().getName() + "_answereddate", sqlquestionmain, "DATETIME", pfolder);
		dimanswerdate.setFolder(pfolder);
		dimensions.add(dimanswerdate);
		
		sqlquestionmain = this.getMainTable().getName() +".S_ASSESSMENTNAME";
		CDimension dimassessname = new CDimension( "Assessment Name", this.getMainTable().getName() + "_assessmentname", sqlquestionmain, "STRING", pfolder);
		dimassessname.setFolder(pfolder);
		dimensions.add(dimassessname);
		
		sqlquestionmain = this.getMainTable().getName() +".S_SEQUENCENUMBER";
		CDimension dimseq = new CDimension( "Sequence #", this.getMainTable().getName() + "_sequencenumber", sqlquestionmain, "INTEGER", pfolder);
		dimseq.setFolder(pfolder);
		dimensions.add(dimseq);
		
		sqlquestionmain = this.getMainTable().getName() +".S_STARTDATE";
		CDimension dimstartdate = new CDimension( "Start Date", this.getMainTable().getName() + "_startdate", sqlquestionmain, "DATETIME", pfolder);
		dimstartdate.setFolder(pfolder);
		dimensions.add(dimstartdate);
		
		sqlquestionmain = this.getMainTable().getName() +".S_DUEDATE";
		CDimension dimduedate = new CDimension( "Due Date", this.getMainTable().getName() + "_duedate", sqlquestionmain, "DATETIME", pfolder);
		dimduedate.setFolder(pfolder);
		dimensions.add(dimduedate);
		
		sqlquestionmain = this.getMainTable().getName() +".S_HASCREATEDRETEST";
		CDimension dimretst = new CDimension( "Retest", this.getMainTable().getName() + "_retest", sqlquestionmain, "INTEGER", pfolder);
		dimretst.setFolder(pfolder);
		dimensions.add(dimretst);
		
		sqlquestionmain = this.getMainTable().getName() +".S_STATUSNAME";
		CDimension dimstatus = new CDimension( "Status Name", this.getMainTable().getName() + "_statusname", sqlquestionmain, "STRING", pfolder);
		dimstatus.setFolder(pfolder);
		dimensions.add(dimstatus);
		
		sqlquestionmain = this.getMainTable().getName() +"_status.IDENTIFIER";
		CDimension dimstatusident = new CDimension( "Status Name (Identifier)", this.getMainTable().getName() + "_statusname_identifier", sqlquestionmain, "STRING", pfolder);
		dimstatusident.setFolder(pfolder);
		dimensions.add(dimstatusident);
		
		
		List<Map<String, String>> listquestions = Main.dm.getQuestion(xtablequestion);
		
		

		
		//Folder answerfolder = Main.test.addClass(this.name);
		
		//Folder answerfolder2 = test.addClass("Answers",answerfolder);
		
		for (Map<String, String> question : listquestions) {
			CTable t = null;
			parentLogger.log(Level.INFO,"testing...");
			if(question.get("MULTIPLEELEMENTS").equals("1") && question.get("TYPENAME").equals("Document")) {
				
				String qtablename = this.getMainTable().getName() + "_"+question.get("LABEL");
				t = new CTable(qtablename, xanswertable, this.name+"_"+question.get("LABEL"));
				
				qtablename = t.getName();
				
				t.addToView("ControlAssessment");
				
				
				t.setSQLQuery("select s."+this.maintable.getPrimaryKey()+", D.*\r\n" + 
						"        from "+xanswertable+" s\r\n" + 
						"        inner join t_document d on d.documentid = s.answerid and d.islive=1\r\n" + 
						"        where questionidentifier = '"+question.get("LABEL")+"'");
				
				parentLogger.log(Level.INFO,"adding MV table " + t.getName());
				
				CJoin j = new CJoin(this.maintable.getName(), this.maintable.getPrimaryKey(), qtablename,  this.maintable.getPrimaryKey());
				this.joins.add(j);
				
				this.questiontables.add(t);
				
				String sqlquestion = qtablename+".DOCUMENTBWID";
				CDimension dimbwid = new CDimension(question.get("DISPLAYSTRING") + " (BWID)", this.name+"." + question.get("LABEL")+"_bwid", sqlquestion, "STRING", pfolder);
				dimbwid.setFolder(pfolder);
				dimensions.add(dimbwid);
				
				sqlquestion = qtablename+".FILENAME";
				CDimension dimf = new CDimension(question.get("DISPLAYSTRING") + " (Filename)", this.name+"." + question.get("LABEL")+"_filename", sqlquestion, "STRING", pfolder);
				dimf.setFolder(pfolder);
				dimensions.add(dimf);
				
				sqlquestion = qtablename+".DISPLAYSTRING";
				CDimension dim = new CDimension(question.get("DISPLAYSTRING") , this.name+"." + question.get("LABEL"), sqlquestion, "STRING", pfolder);
				dim.setFolder(pfolder);
				dimensions.add(dim);
				
			}

			String sqlquestion = this.maintable.getName()+".Q_"+ question.get("LABEL");
			

			
			if(question.get("MULTIPLEELEMENTS").equals("0"))
			switch(question.get("TYPENAME")) {
			case  "Text Line":
			case "Text":

			CDimension dim = new CDimension(question.get("DISPLAYSTRING"), this.name+"." + question.get("LABEL"), sqlquestion, "STRING", pfolder);
			dim.setFolder(pfolder);
			dimensions.add(dim);
			break;
			
			case "Enumeration":

				CDimension dim_ident = new CDimension(question.get("DISPLAYSTRING") +" (identifier)", this.name+"." + question.get("LABEL")+"_identifier", sqlquestion, "STRING", pfolder);
				dim_ident.setFolder(pfolder);
				dimensions.add(dim_ident);
				
				sqlquestion = this.maintable.getName()+".N_"+ question.get("LABEL");
				CDimension dimn = new CDimension(question.get("DISPLAYSTRING") , this.name+"." + question.get("LABEL"), sqlquestion, "STRING", pfolder);
				dimn.setFolder(pfolder);
				dimensions.add(dimn);
				break;
			}
				
			
		}
		
		return dimensions;
		
	}
	public String getName() {
		return name;
	}
	public void setName(String pname) {
		name = pname;
	}
	
	@Override
    public String toString() {
		return name;
	}
	
	@Override
	public int hashCode() {
	    return name.hashCode();
	}
	
	@Override
    public boolean equals(Object o) {
		// If the object is compared with itself then return true   
        if (o == this) { 
            return true; 
        } 
        
        /* Check if o is an instance of Complex or not 
        "null instanceof [type]" also returns false */
      if (!(o instanceof CClass)) { 
          return false; 
      } 
      
   // typecast o to Complex so that we can compare data members  
      CClass c = (CClass) o; 
      
      return this.name.equals(c.name); 
		
	}

}
