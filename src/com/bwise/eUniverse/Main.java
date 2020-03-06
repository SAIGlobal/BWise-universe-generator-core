
package com.bwise.eUniverse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import com.sap.sl.sdk.authoring.businesslayer.BlItem;
import com.sap.sl.sdk.authoring.businesslayer.Dimension;
import com.sap.sl.sdk.authoring.businesslayer.Folder;
import com.sap.sl.sdk.authoring.datafoundation.Cardinality;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Main {

	static Map<String, String> listalltables;
	static BWiseDM dm;
	static CConfig config ;
static boolean isHistoricDWH;
	protected static final Logger parentLogger = LogManager.getLogger();
	

	
	public static void main(String[] args) throws Exception {

		/*********************************************
		 * 
		 * 
		 * Defining input parameters
		 * 
		 * 
		 **********************************************/

		
config = new CConfig(args);

		
		
		/*********************************************
		 * 
		 * 
		 * Setting up instances of BO SDK and SQL Connection
		 * 
		 * 
		 **********************************************/

		Path path = Paths.get(config.TempFolder);
		// Creating a unique temporary directory to save the Universe
		Path tempDirWithPrefix = Files.createTempDirectory(path, "euniverse");

		// Initializing BO SDK
		BWiseFactory test = new BWiseFactory(config.CMS_LOG_USER, config.CMS_LOG_PASS, config.CMS_LOG_HOST, config.UniverseName, config.ODBCName, config.ODBCUser,
				config.ODBCPwd, tempDirWithPrefix.toString() + "\\");
		// Initializing SQL Connection
		dm = new BWiseDM(config.DBUser, config.DBPwd, config.DBServer, config.DBPort, config.DBName, config.dfs, config.dwh, config.dbtype);

		/*********************************************
		 * 
		 * 
		 * Setup Actions (CREATE / OPEN / EXPORT)
		 * 
		 * 
		 **********************************************/
		// we open an existing Universe only if the Universe path is defined
		if (config.open && config.OpenUniverse != null && config.OpenUniverse.length() > 0) {
			//parentLogger.log(Level.INFO,"Opening existing Universe : " + config.OpenUniverse);
			//test.openUNX(config.OpenUniverse);
		} else if (config.create) {
			parentLogger.log(Level.INFO,"Creating new Universe");
			test.createConnection();
			test.createDF();
			test.createBL();
		} else if (config.export && config.OpenUniverse != null && config.OpenUniverse.length() > 0) {
			parentLogger.log(Level.INFO,"Exporting : " + config.OpenUniverse);
			test.openUNX(config.OpenUniverse);
			test.openBL();
			// Fetching all dimensions in the BL
			List<ArrayList<String>> array = exportBL(test.businessLayer.getRootFolder());
			// Insert them into the DB
			if (config.portletmode)
				dm.insert(array, config.UniverseName);
			test.tearDown();
			parentLogger.log(Level.INFO,"FINISHED");
			return;
		} else {
			parentLogger.log(Level.INFO,"Nothing happened. Check Action parameter and Path ");
			return;
		}

		/*********************************************
		 * 
		 * 
		 * CREATE / OPEN Actions main loops
		 * 
		 * 
		 **********************************************/


		parentLogger.log(Level.INFO,"config.OpenUniverse : "+config.OpenUniverse);
		test.openUNX(config.OpenUniverse);
		
		
		
		CMoulinette moulinette= null;
		// If 1 or more BWise class are specified, we fetch de BWise MetaModel
		if ((config.classes != null && config.classes.length() > 0) && !config.classes.equals("none")) {
			isHistoricDWH = dm.isHistoricDWH();

			listalltables = dm.getTablesfromDWH();
	
			
			moulinette = new CMoulinette(config.classes);
			parentLogger.log(Level.INFO,moulinette.getClasses().toString());
			
			
			List<CClass> allClasses = new ArrayList<CClass>();
			List<CTable> allTables = new ArrayList<CTable>();
			List<CDimension> allDimensions = new ArrayList<CDimension>();
			List<CJoin> allJoins = new ArrayList<CJoin>();
			
			// Workflow
			CTable wft = moulinette.buildWorkflow();
			allTables.add(wft);
			
			// RiskTree
			
			if(Main.config.risktree) {
			allTables.add(moulinette.buildRiskTree());
			allDimensions.addAll(moulinette.getRiskTreeDimensions());
			}

			
			allClasses = moulinette.getClasses();
			
			if(!Main.config.controlassessment.equals("none"))
				allClasses.addAll(moulinette.buildControlAssessment());
			
			if(Main.config.multistep) {
				parentLogger.log(Level.INFO,"Adding OA !");
				allClasses.addAll(moulinette.buildOA());
			}
			
	
		    

		    
		    for (Entry<String, String> entry : Main.config.clones.entrySet()) {
		        String key = entry.getKey();
		        String value = entry.getValue();
		        allClasses.add(moulinette.cloneClass(value, key));
		    }
			for(CClass c : allClasses) {
				
				// Main Derived Table for the Class
				allTables.add( c.getMainTable());
				
				
				//Workflow table
				if(c.getRootClass().equals("Issue")) {
					CTable wf = moulinette.buildWorkflowTable(c.getMainTable().getName()+"_WORKFLOW",wft,c.getName());
					wf.addToView("BWise");
					allTables.add( wf);
					
					CJoin njoin = new CJoin(wf.getName(),wf.getPrimaryKey(), c.getMainTable().getName(), c.getMainTable().getPrimaryKey());
					allJoins.add(njoin);
					
					allDimensions.addAll(moulinette.getWorkflowDimensions(wf));
					
				}
				
				
				// Adding N table if needed
				CTable nTable = c.buildNTable();
				if (nTable != null) {
					allTables.add(nTable);
					CJoin njoin = new CJoin(nTable.getName(),nTable.getPrimaryKey(), c.getMainTable().getName(), c.getMainTable().getPrimaryKey());
					allJoins.add(njoin);
					allDimensions.addAll(nTable.getNTableDimensions());
				}
				
				// Adding N table if needed
				CTable wrapperTable = c.buildWrapperTable();
				if (wrapperTable != null) {
					allTables.add(wrapperTable);
					
					CJoin njoin = new CJoin(c.getMainTable().getName(), c.getMainTable().getPrimaryKey(),wrapperTable.getName(),wrapperTable.getPrimaryKey());
					allJoins.add(njoin);
					
					
					if (Main.config.risktree) {
					CJoin treejoin = new CJoin(wrapperTable.getName()+".GROUPID = DT_RISKTREE.GROUPID AND ("+wrapperTable.getName()+".ENTITIESID IN (DT_RISKTREE.RISKID, DT_RISKTREE.CONTROLID))");
					treejoin.setRightTable("DT_RISKTREE");
					treejoin.setLeftTable(wrapperTable.getName());
					allJoins.add(treejoin);
					}
					
					
					
				}
				
				// EnumList tables
				c.getMainTable().fetchEnumListTables();
				allTables.addAll( c.getMainTable().getEnumListTables());
				allJoins.addAll(c.getMainTable().getEnumListJoins());
				allDimensions.addAll(c.getMainTable().getDimensions(c.getMainTable()));

				//Cross Tables (X_)
				allTables.addAll( c.getCrossTable());
				for(CTable t : c.getCrossTable()) {
					//test.addDT(t.getName(), t.getDefaultSQL(), t.getNameRaw());
					t.fetchEnumListTables();
					allTables.addAll( t.getEnumListTables());
					allJoins.addAll(t.getEnumListJoins());
					
					
					allDimensions.addAll(t.getDimensions(c.getMainTable()));

					
					
				}
				
				// Questions
				if(!Main.config.controlassessment.equals("none")) {
				allDimensions.addAll(c.getCAQuestions(c.getName()));
				allTables.addAll( c.getQuestionTables());
				
				}
				
				allJoins.addAll(c.getJoins());
			}
			
		
			
			test.openDFX();
			
			test.addViewToDF("BWise");
			
			
			
			if(!config.controlassessment.equals("none"))
				test.addViewToDF("ControlAssessment");
			
			if(config.multistep) {
				test.addViewToDF("OA");
			}
			

			for(CTable t : allTables) {
				if (t.isAlias())
					test.addAlias(t.getName(),t.getAliasTable());
				else
				test.addDT(t.getName(), t.getSQLQuery(), t.getName());
				for(String view : t.getView()) {
					test.addTableToView(view, t.getName());
				}
			}
//			test.addTableToView("BWise", "DT_RISKTREE");
			for(CJoin j : allJoins) {
				//if (j.getSQLJoin() != null && j.getSQLJoin().length() > 0)
					test.addJoin(j.getSQLJoin(), j.getLeftTable(), j.getRightTable(), Cardinality.C1_N);
			}
			
			test.saveDFX();
			
			test.openBL();
			for(CDimension t : allDimensions) {
				Folder parent = test.addClass(t.parent);
				Dimension d = test.addDimension(t.getName(), parent, t.getType(), t.getSelect(), t.getMapping());
				if (d != null) {
					test.addProperty("BWiseClass", t.getBWiseClass(), d);
					test.addProperty("BWiseMapping", t.getMapping(), d);
					
					
				}
			}
			test.saveBL();
			
			//test.checkIntegrity_Businesslayer();
			
			test.publish();
			
			//tables = dm.getDM(listclasses);
		}
		else {

			parentLogger.log(Level.INFO,"No BWise classes defined in Parameters");

			test.openBL();
			parentLogger.log(Level.INFO,"Exporting....");
			List<ArrayList<String>> array = exportBL(test.businessLayer.getRootFolder());

			if (config.portletmode)
				dm.insert(array, config.UniverseName);

			test.publish();

			test.tearDown();

			parentLogger.log(Level.INFO,"FINISHED");

			return;
		}

		
		
		
		
	}
	

	public static List<ArrayList<String>> exportBL(Folder root) {

		List<ArrayList<String>> dims = new ArrayList<>();
		for (BlItem i : root.getChildren()) {

			parentLogger.log(Level.TRACE,"\n- item = " + i.getName());
			if (i instanceof Folder) {
				List<ArrayList<String>> array1 = exportBL((Folder) i);
				dims = Stream.concat(array1.stream(), dims.stream()).collect(Collectors.toList());

			} else {
				Dimension mydim = (Dimension) i;

				if (mydim.getMapping() != null && mydim.getMapping().length() > 0) {
					ArrayList<String> dim = new ArrayList<String>();
					dim.add(mydim.getParent().getDescription());
					dim.add(mydim.getMapping());
					parentLogger.log(Level.TRACE,"adding dim= " + dim.get(0) + dim.get(1));
					dims.add(dim);
				}

			}

		}

		parentLogger.log(Level.TRACE,"size of dim= " + dims.size());
		return dims;
	}


}
