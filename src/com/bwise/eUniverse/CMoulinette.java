package com.bwise.eUniverse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CMoulinette {
	protected static final Logger parentLogger = LogManager.getLogger();

	List<CClass> classes;
	
	CMoulinette (String pclasses) {
		classes = new ArrayList<CClass>();
		
		List<String> listclasses =
				  Stream.of(pclasses.split(";"))
				  .collect(Collectors.toList());
		
		for(String p : listclasses) {
			CClass bwiseclass = new CClass(p);
			
			if (!classes.contains(bwiseclass)) {
				bwiseclass.fillIt();
				classes.add(bwiseclass);
			}
		}
		
		parentLogger.log(Level.INFO,"Number of classes : "+classes.size());
		
		//tables = Main.dm.getDM(listclasses);
	}
	public CTable buildWorkflow() {
		CTable t = new CTable("TEMPLATESIGNOFF","A_WORKFLOW", "TEMPLATESIGNOFF");

		t.setAsDimension();
		
		t.setSQLQuery(Main.dm.getWorkflowSQL());
		
		return t;
	}
	public CTable buildWorkflowTable(String pName, CTable tw, String pClass) {
		CTable t = new CTable(pName,tw, pClass);

//		t.setAsDimension();
//		t.setPrimaryKey("PROPOSALDWHID");
//		t.setSQLQuery(Main.dm.getWorkflowSQL());
//		
		return t;
	}
	public List<CDimension> getWorkflowDimensions(CTable t) {
		List<CDimension> dimensions = new ArrayList<CDimension>();
		
	
		CDimension dimsubject = new CDimension("Step Name", t.getName() + "_workflow.StepName", t.getName() + ".STEPLABEL", "STRING", t.getBWiseClass());
		dimsubject.setFolder(t.getBWiseClass());
		dimensions.add(dimsubject);
		
		CDimension dimstepname = new CDimension("Previous Step Name", t.getName() + "_workflow.PreviousStepName", t.getName() + ".PREVIOUSSTEPLABEL", "STRING", t.getBWiseClass());
		dimstepname.setFolder(t.getBWiseClass());
		dimensions.add(dimstepname);
		
		CDimension dimperf = new CDimension("Step Performer", t.getName() + "_workflow.StepPerformer", t.getName() + ".ACTIONBY", "STRING", t.getBWiseClass());
		dimperf.setFolder(t.getBWiseClass());
		dimensions.add(dimperf);
		
		CDimension dimdate = new CDimension("Step Date", t.getName() + "_workflow.StepDate", t.getName() + ".STEP_DATE", "DATETIME", t.getBWiseClass());
		dimdate.setFolder(t.getBWiseClass());
		dimensions.add(dimdate);
		
		CDimension dimcom = new CDimension("Action comment", t.getName() + "_workflow.Actioncomment", t.getName() + ".ACTIONCOMMENT", "TEXT", t.getBWiseClass());
		dimcom.setFolder(t.getBWiseClass());
		dimensions.add(dimcom);
		
		CDimension dimstat = new CDimension("Action status", t.getName() + "_workflow.Actionstatus", t.getName() + ".ACTIONSTATUS", "STRING", t.getBWiseClass());
		dimstat.setFolder(t.getBWiseClass());
		dimensions.add(dimstat);

		
		return dimensions;
	}
	public CTable buildRiskTree() {
		CTable t = new CTable("DT_RISKTREE","A_RISKTREE", "A_RISKTREE");
		t.addToView("BWise");
		
		if(!Main.config.controlassessment.equals("none"))	
			t.addToView("ControlAssessment");
		t.setAsDimension();
		
		
		return t;
	}
	

	
	public List<CDimension> getRiskTreeDimensions() {
		List<CDimension> dimensions = new ArrayList<CDimension>();
		
		String sqlquestionmain = "DT_RISKTREE.RISKTREE";
		CDimension dimsubject = new CDimension( "Risk Tree ID", "risktree_id", sqlquestionmain, "INTEGER", "RiskTree");
		dimsubject.setFolder("RiskTree");
		dimensions.add(dimsubject);
		
		sqlquestionmain = "CASE WHEN DT_RISKTREE.RISKTREE = 1 THEN 'Accounts – Risk Tree' "
				+" WHEN DT_RISKTREE.RISKTREE = 2 THEN 'Process Hierarchies – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 3 THEN 'Systems – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 4 THEN 'Accounts – Objectives – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 5 THEN 'Hierarchies – Objectives – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 6 THEN 'Systems – Objectives – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 7 THEN 'Control Objectives – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 8 THEN 'Business Cases – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 9 THEN 'Risks – Risk Tree'"
				+" WHEN DT_RISKTREE.RISKTREE = 100 THEN 'Audit'"
				+" WHEN DT_RISKTREE.RISKTREE = 101 THEN 'Audit Plan'"
				+" WHEN DT_RISKTREE.RISKTREE = 200 THEN 'Risk Validation'"
				+ "END";
		CDimension dimtreename = new CDimension( "Risk Tree Name", "risktree_name", sqlquestionmain, "STRING", "RiskTree");
		dimtreename.setFolder("RiskTree");
		dimensions.add(dimtreename);
		
		return dimensions;
	}
	public List<CClass> getClasses() {
		return classes;
	}
	public CClass cloneClass(String pclass, String pnewClass) {
		CClass oaAggS = new CClass(pclass, pnewClass);
		
		CTable ts1 = new CTable(pnewClass,oaAggS.getMainTable(),pnewClass);
		ts1.setNotAlias();
		ts1.setBWiseClassOriginal(pclass);
		oaAggS.setMainTable(ts1);
		oaAggS.fillIt();
		oaAggS.setBWiseClassOriginal(pclass);
		
		return oaAggS;
	}
	public List<CClass> buildOA() {
		List<CClass> classesoa = new ArrayList<CClass>();
		
		CClass oaAgg = new CClass("BWAggregateAssessment");
		oaAgg.fillIt();
		oaAgg.getMainTable().addToView("OA");
		classesoa.add(oaAgg);
		
		CClass oaAggS = new CClass("BWAggregateAssessmentStep", "DT_STEP1");
		//oaAgg.getMainTable().setName("DT_STEP1");
		oaAggS.fillIt();
		oaAggS.getMainTable().addToView("OA");
		
		
		CJoin JStep1 = new CJoin(oaAggS.getMainTable().getName(), "AGGREGATEID", oaAgg.getMainTable().getName(), oaAgg.getMainTable().getPrimaryKey());
		oaAggS.addJoin(JStep1);
		
		classesoa.add(oaAggS);
		
		CClass oaAggSASS = new CClass("BWAssessment", "DT_STEP1_ASSESSMENT");
		//oaAgg.getMainTable().setName("DT_STEP1");
		oaAggSASS.fillIt();
		oaAggSASS.getMainTable().addToView("OA");
		oaAggSASS.getMainTable().setSQLQuery("select T.*,x.BWaggregateAssessmentStepId\r\n" + 
				"from t_bwassessment T\r\n" + 
				"inner join x_bwaggregtssssmntstp_ssssmnts x on x.islive=1 and t.bwassessmentid = x.relatedid\r\n" + 
				"where t.islive = 1");
		
		
		CJoin JStep11 = new CJoin(oaAggSASS.getMainTable().getName(), "AGGREGATESTEPID", oaAggS.getMainTable().getName(), oaAggS.getMainTable().getPrimaryKey());
		oaAggSASS.addJoin(JStep11);
		
		classesoa.add(oaAggSASS);
		
		
		CClass oaAggSsess = new CClass("BWAssessmentSession", "DT_STEP1_SESSION");
		oaAggSsess.fillIt();
		oaAggSsess.getMainTable().addToView("OA");
		oaAggSsess.getMainTable().setSQLQuery("select  a.GROUPID, t.*\r\n" + 
				"from t_bwassessmentsession t\r\n" + 
				"inner join t_bwassessment a on a.islive=1 and a.bwassessmentid = t.assessmentid\r\n" + 
				"where t.islive = 1");
		
		CJoin JStep11S = new CJoin(oaAggSsess.getMainTable().getName(), "ASSESSMENTID", oaAggSASS.getMainTable().getName(), oaAggSASS.getMainTable().getPrimaryKey());
		oaAggSsess.addJoin(JStep11S);
		
		classesoa.add(oaAggSsess);
		
		
		
		CClass oaAggSLink = new CClass("BWAggregateSessionLink", "DT_STEP1_LINK");

		oaAggSLink.fillIt();
		oaAggSLink.getMainTable().addToView("OA");
		oaAggSLink.getMainTable().setNoDimensions();
		
		
		CClass oaAggAns = new CClass("BWAnswer", "DT_STEP1_ANSWERS");
		oaAggAns.fillIt();
		oaAggAns.getMainTable().addToView("OA");

		
		CJoin JAns= new CJoin(oaAggAns.getMainTable().getName(), "ASSESSMENTSESSIONID", oaAggSsess.getMainTable().getName(), oaAggSsess.getMainTable().getPrimaryKey());
		oaAggAns.addJoin(JAns);
		
		classesoa.add(oaAggAns);
		
		
		int start = 2;
		int stepnumber = Main.config.stepsnumber;
		
		for (int s = start;s<=stepnumber; s++) {
			


		
	
			
		CClass oaAggS1 = new CClass("BWAggregateAssessmentStep", "DT_STEP"+s);
		CTable ts1 = new CTable("DT_STEP"+s,oaAggS.getMainTable(),"DT_STEP"+s);
		oaAggS1.setMainTable(ts1);
		oaAggS1.fillIt();
		oaAggS1.getMainTable().addToView("OA");
		classesoa.add(oaAggS1);
		
		CClass oaAggS1ass = new CClass("BWAssessment", "DT_STEP"+s + "_ASSESSMENT");
		CTable ts1ass = new CTable("DT_STEP"+s+ "_ASSESSMENT",oaAggSASS.getMainTable(),"DT_STEP"+s+ "_ASSESSMENT");
		oaAggS1ass.setMainTable(ts1ass);
		oaAggS1ass.fillIt();
		oaAggS1ass.getMainTable().addToView("OA");
		
		
		CJoin JStep11n = new CJoin(oaAggS1ass.getMainTable().getName(), "AGGREGATESTEPID", oaAggS1.getMainTable().getName(), oaAggS1.getMainTable().getPrimaryKey());
		oaAggS1ass.addJoin(JStep11n);
		
		classesoa.add(oaAggS1ass);
		
		
		CClass oaAggS1sess = new CClass("BWAssessmentSession", "DT_STEP"+s+ "_SESSION");
		CTable ts1ses = new CTable("DT_STEP"+s+ "_SESSION",oaAggSsess.getMainTable(),"DT_STEP"+s+ "_SESSION");
		oaAggS1sess.setMainTable(ts1ses);
		oaAggS1sess.fillIt();
		oaAggS1sess.getMainTable().addToView("OA");
		classesoa.add(oaAggS1sess);
		
		CJoin JStep11Sn = new CJoin(oaAggS1sess.getMainTable().getName(), "ASSESSMENTID", oaAggS1ass.getMainTable().getName(), oaAggS1ass.getMainTable().getPrimaryKey());
		oaAggS1sess.addJoin(JStep11Sn);
		
		
		CClass oaAggAns1 = new CClass("BWAnswer", "DT_STEP"+s+"_ANSWERS");
		CTable ts1ans = new CTable("DT_STEP"+s+ "_ANSWERS",oaAggAns.getMainTable(),"DT_STEP"+s+ "_ANSWERS");
		oaAggAns1.setMainTable(ts1ans);
		oaAggAns1.fillIt();
		oaAggAns1.getMainTable().addToView("OA");

		
		CJoin JAns1= new CJoin(oaAggAns1.getMainTable().getName(), "ASSESSMENTSESSIONID", oaAggS1sess.getMainTable().getName(), oaAggS1sess.getMainTable().getPrimaryKey());
		oaAggAns1.addJoin(JAns1);
		
		classesoa.add(oaAggAns1);
		
		
		CClass oaAggS1Link = null;
	if(s == start) {
		oaAggS1Link = oaAggSLink;
		
		CJoin JLink1 = new CJoin(oaAggS1Link.getMainTable().getName(), "OPENSESSIONFROMID", oaAggSsess.getMainTable().getName(), oaAggSsess.getMainTable().getPrimaryKey());
		oaAggS1Link.addJoin(JLink1);
		

	} else {
		oaAggS1Link = new CClass("BWAggregateSessionLink", "DT_STEP"+(s-1) + "_LINK");
		CTable ts1link = new CTable("DT_STEP"+(s-1)+ "_LINK",oaAggSLink.getMainTable(),"DT_STEP"+(s-1)+ "_LINK");
		ts1link.setNoDimensions();
		oaAggS1Link.setMainTable(ts1link);
		oaAggS1Link.fillIt();
		oaAggS1Link.getMainTable().addToView("OA");
		
		CJoin JLink1 = new CJoin(oaAggS1Link.getMainTable().getName(), "OPENSESSIONFROMID", "DT_STEP"+(s-1)+ "_SESSION", ts1ses.getPrimaryKey());
		oaAggS1Link.addJoin(JLink1);
		
		
	}
	
	
	CJoin JLink2 = new CJoin(ts1ses.getName(), ts1ses.getPrimaryKey() , oaAggS1Link.getMainTable().getName(), "OPENSESSIONTOID");
	oaAggS1Link.addJoin(JLink2);
	
	classesoa.add(oaAggS1Link);
	
	
		
		
		}
		
		
		

		
		return classesoa;
	}
	
	public List<CClass> buildControlAssessment() {
		List<CClass> classescm = new ArrayList<CClass>();
		
		/*
		 * listclasses.add("BWCntrlMonitorAssm");
			listclasses.add("BWCntrlMonitorSession");
			
			listclasses.add("BWCntrlReviewAssm");
			listclasses.add("BWCntrlReviewSession");
			
			listclasses.add("BWCntrlTestAssm");
			listclasses.add("BWCntrlTestSession");
		 * */
		 
		
		// MONITOR
		CClass cma = new CClass("BWCntrlMonitorAssm");
		cma.fillIt();

		cma.getMainTable().setSQLQuery("SELECT T.* \r\n" + 
				", x.RELATEDID FROM T_BWCNTRLMONITORASSM T \r\n" + 
				" INNER JOIN X_BWCNTRLMONITORASSM_SUBJECTS X on X.BWCNTRLMONITORASSMID = T.BWCNTRLMONITORASSMID AND X.ISLIVE=1WHERE T.ISLIVE=1");
		
		
		if  (Main.config.risktree) { 
		CJoin jm = new CJoin(cma.getMainTable().getName()+".RELATEDID=DT_RISKTREE.CONTROLID AND "+cma.getMainTable().getName()+".GROUPID=DT_RISKTREE.GROUPID");
		jm.setLeftTable(cma.getMainTable().getName());
		jm.setRightTable("DT_RISKTREE");
		cma.addJoin(jm);
		}
		cma.getMainTable().addToView("ControlAssessment");
		classescm.add(cma);
		
		// MONITOR SESSION
		CClass cms = new CClass("BWCntrlMonitorSession","SM_"+Main.config.controlassessment.toUpperCase());
		cms.fillIt();
		cms.getMainTable().setAsQuestionTable();
		cms.getMainTable().setSQLTable("SM_"+Main.config.controlassessment.toUpperCase());
		cms.getMainTable().addToView("ControlAssessment");
		cms.getMainTable().setNoDimensions();
		
		
		CJoin jms = new CJoin(cms.getMainTable().getName() + ".S_ASSESSMENTID" + "=" + cma.getMainTable().getName() + ".BWCNTRLMONITORASSMID AND " + cms.getMainTable().getName() + ".S_SUBJECTID="+cma.getMainTable().getName()+".RELATEDID");
		jms.setLeftTable(cms.getMainTable().getName());
		jms.setRightTable(cma.getMainTable().getName() );
		cms.addJoin(jms);
		
		
		classescm.add(cms);
		
		//REVIEW
		CClass cra = new CClass("BWCntrlReviewAssm");
		cra.fillIt();
		CJoin jra = new CJoin(cra.getMainTable().getName(), "CONTROLMONITORINGASSESSMENTID", cma.getMainTable().getName(), "BWCNTRLMONITORASSMID");
		cra.addJoin(jra);
		cra.getMainTable().addToView("ControlAssessment");
		
		classescm.add(cra);
		
		//REVIEW SESSION
		CClass crs = new CClass("BWCntrlReviewSession","SV_"+Main.config.controlassessment.toUpperCase());
		crs.fillIt();
		crs.getMainTable().setAsQuestionTable();
		crs.getMainTable().setSQLTable("SV_"+Main.config.controlassessment.toUpperCase());
		crs.getMainTable().addToView("ControlAssessment");
		crs.getMainTable().setNoDimensions();
		
		CJoin jrs = new CJoin(crs.getMainTable().getName(), "S_MONITORINGSESSIONID", cms.getMainTable().getName(), "BWCNTRLMONITORSESSIONID");
		crs.addJoin(jrs);
		classescm.add(crs);
		


		
		
		
		// TEST
		
		CClass cta = new CClass("BWCntrlTestAssm");
		cta.fillIt();
		cta.getMainTable().addToView("ControlAssessment");

		CJoin jta = new CJoin(cta.getMainTable().getName(), "CONTROLREVIEWASSESSMENTID", cra.getMainTable().getName(), "BWCNTRLREVIEWASSMID");
		cta.addJoin(jta);

		
		classescm.add(cta);
		
		// TEST SESSION
		CClass cts = new CClass("BWCntrlTestSession","ST_"+Main.config.controlassessment.toUpperCase());
		cts.fillIt();
		cts.getMainTable().addToView("ControlAssessment");
		cts.getMainTable().setAsQuestionTable();
		cts.getMainTable().setSQLTable("ST_"+Main.config.controlassessment.toUpperCase());
		cts.getMainTable().setNoDimensions();
		
		CJoin jts = new CJoin(cts.getMainTable().getName(), "S_REVIEWSESSIONID", crs.getMainTable().getName(), "BWCNTRLREVIEWSESSIONID");
		cts.addJoin(jts);
		
		classescm.add(cts);
		
	
		
		return classescm;
	}
}
