package com.bwise.eUniverse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.crystaldecisions.sdk.exception.SDKException;
import com.crystaldecisions.sdk.framework.CrystalEnterprise;
import com.crystaldecisions.sdk.framework.IEnterpriseSession;
import com.crystaldecisions.sdk.plugin.authentication.enterprise.IsecEnterpriseBase;
import com.sap.sl.sdk.authoring.businesslayer.AccessLevel;
import com.sap.sl.sdk.authoring.businesslayer.BlContainer;
import com.sap.sl.sdk.authoring.businesslayer.BlItem;
import com.sap.sl.sdk.authoring.businesslayer.BusinessLayerFactory;
import com.sap.sl.sdk.authoring.businesslayer.DataType;
import com.sap.sl.sdk.authoring.businesslayer.Dimension;
import com.sap.sl.sdk.authoring.businesslayer.Folder;
import com.sap.sl.sdk.authoring.businesslayer.ItemState;
import com.sap.sl.sdk.authoring.businesslayer.RelationalBinding;
import com.sap.sl.sdk.authoring.businesslayer.RelationalBusinessLayer;
import com.sap.sl.sdk.authoring.checkintegrity.CheckIntegrityRunner;
import com.sap.sl.sdk.authoring.cms.CmsResourceService;
import com.sap.sl.sdk.authoring.connection.ConnectionFactory;
import com.sap.sl.sdk.authoring.connection.RelationalConnection;
import com.sap.sl.sdk.authoring.datafoundation.AliasTable;
import com.sap.sl.sdk.authoring.datafoundation.Cardinality;
import com.sap.sl.sdk.authoring.datafoundation.DataFoundationFactory;
import com.sap.sl.sdk.authoring.datafoundation.DataFoundationView;
import com.sap.sl.sdk.authoring.datafoundation.DateValue;
import com.sap.sl.sdk.authoring.datafoundation.DerivedTable;
import com.sap.sl.sdk.authoring.datafoundation.MonoSourceDataFoundation;
import com.sap.sl.sdk.authoring.datafoundation.OuterType;
import com.sap.sl.sdk.authoring.datafoundation.Parameter;
import com.sap.sl.sdk.authoring.datafoundation.SQLJoin;
import com.sap.sl.sdk.authoring.datafoundation.SingleValueAnswer;
import com.sap.sl.sdk.authoring.datafoundation.Join;
import com.sap.sl.sdk.authoring.datafoundation.LovParameterDataType;
import com.sap.sl.sdk.authoring.datafoundation.Table;
import com.sap.sl.sdk.authoring.datafoundation.TableState;
import com.sap.sl.sdk.authoring.datafoundation.TableView;
import com.sap.sl.sdk.authoring.local.LocalResourceService;
import com.sap.sl.sdk.framework.IStatus;
import com.sap.sl.sdk.framework.SlContext;
import com.sap.sl.sdk.framework.cms.CmsSessionService;

public class BWiseFactory {
	
	protected static final Logger parentLogger = LogManager.getLogger();
		/** Authentication mode used to log in to the CMS. Here: Enterprise */
	    private  String CMS_AUTH_MODE = IsecEnterpriseBase.PROGID;

	    
	   /** Database Host name */
    private   String CNX_DATASOURCE;



    /** Database User name */
    private  String CNX_USER;

    /** Database User password */
    private  String CNX_PASS;
    
    /** Local folder used to save all resources locally */
    private  String LOCAL_FOLDER ;
    
    private  SlContext context;
    private  IEnterpriseSession enterpriseSession;

    private DataFoundationFactory dataFoundationFactory;
    BusinessLayerFactory businessLayerFactory;

    private LocalResourceService localResourceService;
    
    private String shortcutPathCNX;
    private CmsResourceService cmsResourceService;
    
    MonoSourceDataFoundation dataFoundation;
    public RelationalBusinessLayer businessLayer;
    RelationalConnection connection;
    ConnectionFactory connectionFactory;
    
    String BLXName;
    String DFXName;
    String CNXName;
    String CNSName;
    String Name;
    
    String blPath;
    
    public  BWiseFactory(String CMS_LOG_USER, String CMS_LOG_PASS, String CMS_LOG_HOST, String UniverseName,String ODBCName, String ODBCUser, String ODBCPwd, String TempFolder) throws Exception {

        // Connects to the CMS and creates a session
        context = SlContext.create();
        enterpriseSession = CrystalEnterprise.getSessionMgr().logon(CMS_LOG_USER, CMS_LOG_PASS, CMS_LOG_HOST, CMS_AUTH_MODE);
        context.getService(CmsSessionService.class).setSession(enterpriseSession);
        dataFoundationFactory = context.getService(DataFoundationFactory.class);
        connectionFactory = context.getService(ConnectionFactory.class);
        businessLayerFactory = context.getService(BusinessLayerFactory.class);
        cmsResourceService = context.getService(CmsResourceService.class);
        localResourceService = context.getService(LocalResourceService.class);
        Name = UniverseName;
        BLXName= "BL"+UniverseName+".blx";
        DFXName = "DF"+UniverseName+".dfx";
        CNXName = "CNX"+UniverseName+".cnx";
        CNSName = "CNX"+UniverseName+".cns";
        
        CNX_DATASOURCE = ODBCName;
        CNX_USER = ODBCUser;
        CNX_PASS = ODBCPwd;
        
        LOCAL_FOLDER = TempFolder;
    }
    
    public void tearDown() throws Exception {
    	if(connection != null)
    	localResourceService.close(connection);
    	localResourceService.close(dataFoundation);
   	 localResourceService.close(businessLayer);
        // Closes the CMS session
        context.close();
        enterpriseSession.logoff();

    }
public void createConnection() {
	   //
    // ** Connection creation
    //

    parentLogger.log(Level.TRACE,"Create new connection");
     
     connection = connectionFactory.createRelationalConnection(Name, "MS SQL Server 2016", "ODBC Drivers");
    connection.getParameter("DATASOURCE").setValue(CNX_DATASOURCE);
    //connection.getParameter("DATABASE").setValue(CNX_DATABASE);
    connection.getParameter("USER_NAME").setValue(CNX_USER);
    connection.getParameter("PASSWORD").setValue(CNX_PASS);

    // Saves the connection locally
    parentLogger.log(Level.TRACE,String.format("Save connection locally into \"%s"+CNXName+"\"", LOCAL_FOLDER));
    localResourceService = context.getService(LocalResourceService.class);
    localResourceService.save(connection, LOCAL_FOLDER + CNXName, true);

    // Publishes the connection to a CMS and retrieves a shortcut

    parentLogger.log(Level.TRACE,String.format("\n\t- Publish connection into \"%s/"+CNXName+"\" and retrieve a shortcut", CmsResourceService.CONNECTIONS_ROOT));
    String cnxCmsPath = cmsResourceService.publish(LOCAL_FOLDER + CNXName, CmsResourceService.CONNECTIONS_ROOT, true);
    shortcutPathCNX = cmsResourceService.createShortcut(cnxCmsPath, LOCAL_FOLDER);

    parentLogger.log(Level.TRACE,"shortcut in " + shortcutPathCNX);
    
}

public void createDF() {
    //
    // ** Single-source Data Foundation creation
    //

    
    parentLogger.log(Level.TRACE,"Create new data foundation");
  
    dataFoundation = dataFoundationFactory.createMonoSourceDataFoundation(Name, shortcutPathCNX);
    dataFoundation.setName(Name);
   
    parentLogger.log(Level.TRACE,dataFoundation.getName());
    
    addParameter();
    // Saves the data foundation

    parentLogger.log(Level.TRACE,String.format("\n\t- Save data foundation locally into \"%s"+DFXName+"\"", LOCAL_FOLDER));
    
    parentLogger.log(Level.TRACE,LOCAL_FOLDER + DFXName);
    localResourceService.save(dataFoundation, LOCAL_FOLDER + DFXName, true);
    
    
}

public void createBL() {
    //
    // ** Business Layer creation
    //

    

    // Creates the business layer
    parentLogger.log(Level.TRACE,"Create business layer");
     businessLayer = businessLayerFactory.createRelationalBusinessLayer(Name, LOCAL_FOLDER + DFXName);

     blPath = LOCAL_FOLDER + BLXName;

     parentLogger.log(Level.TRACE,"path = "+ blPath);
     businessLayer.setName(Name);
    // Saves the business layer

    parentLogger.log(Level.TRACE,String.format("\n\t- Save business layer locally into \"%s"+BLXName+"\"", LOCAL_FOLDER));

    localResourceService.save(businessLayer, LOCAL_FOLDER + BLXName, true);
    
   

}

public void openUNX(String UNX_PATH) throws SDKException {


	parentLogger.log(Level.INFO,"Opening universe : "+UNX_PATH);
    // Retrieves the specified universe
	blPath = cmsResourceService.retrieveUniverse(UNX_PATH, LOCAL_FOLDER, true);


	parentLogger.log(Level.TRACE,"The universe has been retrieved from the CMS repository and its resources have been created in path: \"" + blPath + "\"");
	
	businessLayer = (RelationalBusinessLayer)  localResourceService.load(blPath);
	
	parentLogger.log(Level.INFO,"Opening universe 2 : "+businessLayer.getDataFoundationPath());
	
	dataFoundation = (MonoSourceDataFoundation) localResourceService.load(businessLayer.getDataFoundationPath());
	

}

public Folder addClass(String ClassName) {
	 // Creates the folder that contains the business layer

    parentLogger.log(Level.TRACE,"Create folder " + ClassName);
    Folder blxFolder1  = (Folder) existFolderDesc(businessLayer.getRootFolder(), ClassName);
    if (blxFolder1 != null) {
    	 parentLogger.log(Level.TRACE,"folder already exists " + ClassName);
    } else {
    blxFolder1 = businessLayerFactory.createBlItem(Folder.class, ClassName, businessLayer.getRootFolder());
    blxFolder1.setDescription(ClassName);
    blxFolder1.setState(ItemState.ACTIVE);
    
    
    businessLayerFactory.createCustomProperty("BWiseSource", ClassName, blxFolder1);
    
    localResourceService.save(businessLayer, blPath, true);
    }
    return blxFolder1;
}

public Folder addClass(String ClassName, Folder parent) {
	 // Creates the folder that contains the business layer
	parentLogger.log(Level.TRACE,"Create folder " + ClassName);
   Folder blxFolder1  = (Folder) existFolderDesc(parent, ClassName);
   if (blxFolder1 != null) {
	   parentLogger.log(Level.TRACE,"folder already exists " + ClassName);
   } else {
   blxFolder1 = businessLayerFactory.createBlItem(Folder.class, ClassName, parent);
   blxFolder1.setDescription(ClassName);
   blxFolder1.setState(ItemState.ACTIVE);
   
   
   businessLayerFactory.createCustomProperty("BWiseSource", ClassName, blxFolder1);
   
   localResourceService.save(businessLayer, blPath, true);
   }
   return blxFolder1;
}

public Dimension addDimension(String Dimension,BlContainer Parent, DataType type, String SQL, String mapping) {
	 // Creates a dimension in the business layer

    parentLogger.log(Level.TRACE,"Create dimension" + Dimension);
    if (existItemMapping(Parent, mapping))

    parentLogger.log(Level.TRACE,"mapping already exist" + SQL);
    
    else {
    	
    	if ( existItemName(Parent, Dimension)   ) {

    	   	parentLogger.log(Level.TRACE,"object name already exists : " + SQL);
    	   	Dimension = Dimension + " - dup - " + this.getAlphaNumericString(3);
    	}
    	
    Dimension blxDimension1 = businessLayerFactory.createBlItem(Dimension.class, Dimension, Parent);
    blxDimension1.setDescription(mapping);
    blxDimension1.setAccessLevel(AccessLevel.PUBLIC);
    blxDimension1.setDataType(type);
    blxDimension1.setState(ItemState.ACTIVE);
    blxDimension1.setMapping(mapping);
    
	SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
	Date date = new Date(System.currentTimeMillis());

	
    blxDimension1.setTechnicalInformation("created on " + formatter.format(date) + " by using eUniverse");
    RelationalBinding binding = (RelationalBinding) blxDimension1.getBinding();
    binding.setSelect(SQL);
    parentLogger.log(Level.TRACE,"dimension created : " + blxDimension1.getName());
    //businessLayerFactory.createCustomProperty("BWiseSource", className, blxDimension1);
    
    return blxDimension1;
    
    }
  return null;
    
  
}

public void addProperty(String name, String value, Dimension blxDimension1) {
	businessLayerFactory.createCustomProperty(name, value, blxDimension1);
}

public boolean findSQL(String SQL,BlItem item2) {

     if (SQL=="2" && item2 instanceof Folder) {
         for (BlItem item : ((BlContainer) item2).getChildren()) {
        	 if (item instanceof Dimension) {
        		 Dimension dim = (Dimension) item;
        		 if (  ((RelationalBinding) dim.getBinding()).getSelect()== SQL)
        			 return true;
        	 } else
             return findSQL(SQL,item2);
             
         }
     }

	return false;
}

// Looks for a business item with the given name in the given container
private  boolean existItemMapping(BlItem container, String name) {

    if (container instanceof Dimension && name.equals(((Dimension) container).getMapping())) {
    	//Dimension dim = (Dimension) container;
    	
    	return true;
    }

    if (container instanceof BlContainer) {
        for (BlItem item : ((BlContainer) container).getChildren()) {
            boolean searchResult = existItemMapping(item, name);
            if (searchResult)
                return searchResult;
        }
    }

    return false;
}

//Looks for a business item with the given name in the given container
private  boolean existItemName(BlItem container, String name) {

 if (name.equals(container.getName()))
     return true;

 if (container instanceof BlContainer) {
     for (BlItem item : ((BlContainer) container).getChildren()) {
         boolean searchResult = existItemName(item, name);
         if (searchResult)
             return searchResult;
     }
 }

 return false;
}

//Looks for a business item with the given name in the given container
private  BlItem existFolderDesc(BlItem container, String name) {

if (container instanceof Folder && (name.equals(container.getName()) || name.equals(((Folder) container).getDescription())))
   return container;

if (container instanceof BlContainer) {
   for (BlItem item : ((BlContainer) container).getChildren()) {
	   BlItem searchResult = existFolderDesc(item, name);
       if (searchResult != null)
           return searchResult;
   }
}

return null;
}

public void openDFX() {
	dataFoundation = (MonoSourceDataFoundation) localResourceService.load(businessLayer.getDataFoundationPath());
}

public void openBL() {
	 businessLayer = (RelationalBusinessLayer) localResourceService.load(blPath);
}
public void saveDFX() {
	
	localResourceService.save(dataFoundation, businessLayer.getDataFoundationPath(), true);
	

}

public void addViewToDF(String name) {
	for(DataFoundationView view : dataFoundation.getDataFoundationViews())
	{
		if (view.getName().equals(name))
			return;
	}
	dataFoundationFactory.createDataFoundationView(name, dataFoundation);
	this.saveDFX();
	
}

public void addTableToView(String viewname, String tableName) {

	parentLogger.log(Level.DEBUG,"ADDING TO VIEW");
	for(DataFoundationView view : dataFoundation.getDataFoundationViews())
	{
		
		if (view.getName().equals(viewname)) {

			parentLogger.log(Level.DEBUG,"FOUND VIEW " + viewname);
			Table t = findTable(tableName);
			
			for(TableView tview : view.getTableViews()) {
				if(tview.getTable().getName().equals(tableName))
					return;
			}

			parentLogger.log(Level.DEBUG,"Adding " + t.getName() + " to view " + viewname);
			TableView tview = dataFoundationFactory.createTableView(t, view);
			tview.setTableState(TableState.JOINS_ONLY);

			return;
		}
	}
	parentLogger.log(Level.WARN,"COULD NOT FIND VIEW " + viewname);

	
}

public void reorderTables () {
	
	openDFX();
    DataFoundationView masterView = dataFoundation.getMasterView();
    Table table = null;
    TableView tableView = null;
    
	for(int i = 0; i < dataFoundation.getTables().size(); i++) {
		table = dataFoundation.getTables().get(i);
		//dataFoundation.getTables().get(0).
		tableView = dataFoundationFactory.createTableView(table, masterView);
		tableView.setX((i * 200) % 1000);
		tableView.setY((i / 5) * 50);
		tableView.setWidth(150);
		tableView.setTableState(TableState.COLLAPSED);
	}
	saveDFX();
}
public void saveBL() {
	localResourceService.save(businessLayer, blPath, true);
}

public boolean tableExists(String name) {
	for (Table t : dataFoundation.getTables()) 
	{
		if (t.getName().equals(name))
			return true;
	}
	return false;
}

public Table findTable(String name) {
	for (Table t : dataFoundation.getTables()) 
	{
		if (t.getName().equals(name))
			return t;
	}
	return null;
}

public boolean tableExists2(String name) {
	for (Table t : dataFoundation.getTables()) 
	{
		if (t.getDescription().equals(name))
			return true;
	}
	return false;
}
public void addDT(String name , String SQL,  String className) {

	  parentLogger.log(Level.TRACE,"add DT  name = " + name + " & classname = "+className);
	if (tableExists(name) || tableExists2(className))
		return;
	try {
		DerivedTable tt = dataFoundationFactory.createDerivedTable(name, SQL, dataFoundation);
	tt.setDescription(className);

	//PrimaryKey pk = dataFoundationFactory.createPrimaryKey(tt );
} catch (Exception e) {
	parentLogger.log(Level.ERROR,"SQL for "+name+" = "+SQL);
	parentLogger.log(Level.ERROR,e.getLocalizedMessage());
	e.printStackTrace();
}

}

public void addDT(String name , String SQL,  String className, String tableName) {
	
	if(Main.listalltables.containsKey(tableName)) {
	
		parentLogger.log(Level.DEBUG,"found table " + tableName + " in the DWH");
	} else {

		parentLogger.log(Level.WARN,"could not find table " + tableName + " in the DWH");
		return;
	}
try {

	  parentLogger.log(Level.TRACE,"add DT  name = " + name + " & classname = "+className);
	if (tableExists(name) || tableExists2(className))
		return;
	if(dataFoundation == null)
	parentLogger.log(Level.ERROR,"Seriously???");
	
	DerivedTable tt = dataFoundationFactory.createDerivedTable(name, SQL, dataFoundation);
	
	
	tt.setDescription(className);
	//PrimaryKey pk = dataFoundationFactory.createPrimaryKey(tt );
} catch (Exception e) {
	parentLogger.log(Level.ERROR,"SQL for "+name+" = "+SQL);
	parentLogger.log(Level.ERROR,e.getLocalizedMessage());
	e.printStackTrace();
}

}

public void addAlias(String name , String ptable) {
	
	if (tableExists(name))
		return;

	AliasTable al = dataFoundationFactory.createAliasTable(name, findTable(ptable), dataFoundation);

	al.setDescription("test");

}

public void checkIntegrity_Businesslayer() throws Exception {
	CheckIntegrityRunner checkIntegrityRunner = localResourceService.createCheckIntegrityRunner(blPath);

	
	
    IStatus status = checkIntegrityRunner.start();
    System.out.println(dumpStatus(status));
} 

private static String dumpStatus(IStatus status) throws Exception {
	if(status==null) throw new Exception("Status is null");
	
	StringBuilder buffer = new StringBuilder();
	dumpStatus(status, buffer, 0);
	return buffer.toString();
}

private static void dumpStatus(IStatus status, StringBuilder buffer, int depth) {
	String newLine = System.getProperty("line.separator");

	if(buffer.length() > 0 ) buffer.append(newLine);
	for(int i = depth; i >= 0; i--) buffer.append("-");
	buffer.append("> " + status.getSeverity().name());
	if(!status.getMessage().equals("")) buffer.append(": " + status.getMessage());
	if(status.getCause()!=null) buffer.append(" " + status.getCause().getMessage());
	if(status.getCode()!=null && !status.getCode().equals("")) buffer.append(" (" + status.getCode() + ")");
	
	if(status.hasChildren()) {
		for(IStatus child : status.getChildren()) {
			dumpStatus(child, buffer, depth + 1);
		}
	}
}

public void addParameter() {
	 Parameter parameter = dataFoundationFactory.createParameter("SD", dataFoundation);
     parameter.setUserPrompted(true);
     parameter.setPromptText("Enter date");
     parameter.setDataType(LovParameterDataType.DATE);
     parameter.setMultipleValuesAllowed(false);
     parameter.setKeepLastValuesEnabled(false);
     
     SingleValueAnswer a = businessLayerFactory.createSingleValueAnswer() ;
     Date date = new Date();
     
     DateValue dv = businessLayerFactory.createDateValue(date);
     
     //TypedValue v = businessLayerFactory.createStringValue("CurrentDate()");
    
     
     a.setValue(dv);
    
     parameter.setDefaultAnswer(a);

}
public void addJoin(String expression, String tab1, String tab2, Cardinality card) {
	  // Creates joins between the tables and sets cardinalities
	
	int flag = 0;
	int flag2 = 0;

	for (Table a : dataFoundation.getTables()) {
		if (a.getName().equals(tab1)) {
			flag = 1;
		}
		if (a.getName().equals(tab2)) {
			flag2 = 1;
		}
	}
	if (flag != 1 ) {

		parentLogger.log(Level.WARN,"missing table "+tab1+" for Join : " + flag + " - " + expression);
		return;
	}
	
	if (flag2 != 1) {

		parentLogger.log(Level.WARN,"missing table "+tab2+" for Join : " + flag + " - " + expression);
		return;
	}
	for (Join t : dataFoundation.getJoins()) 
	{
		SQLJoin j = (SQLJoin) t;
		if (j.getExpression().replace("\"", "").replace(" = ", "=").equals(expression.replace("\"", "").replace(" = ", "=")))
			return;
	}


   
    try {
 SQLJoin join = dataFoundationFactory.createSqlJoin(expression, dataFoundation);
 parentLogger.log(Level.TRACE,"Create joins");
 join.setOuterType(OuterType.OUTER_RIGHT);
    join.setCardinality(card);
    }
    catch(Exception e) {
        parentLogger.log(Level.ERROR,"SQL = " + expression);
        e.printStackTrace();
    }
}


public void addDescription(String desc) {
	businessLayer.setDescription(desc);
}

public void appendDescription(String desc) {
	businessLayer.setDescription(desc +System.lineSeparator()+ businessLayer.getDescription());
}


public void save() {
	localResourceService.save(dataFoundation, businessLayer.getDataFoundationPath(), true);
	
	 // Saves the business layer

	   parentLogger.log(Level.TRACE,String.format("Save business layer locally into \"%s"+BLXName+"\"", LOCAL_FOLDER));
	   localResourceService.save(businessLayer, blPath, true);
}

public void publish() {
    //
    // ** Business Layer publication
    //
	//save();
	
	SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
	Date date = new Date(System.currentTimeMillis());

	
	this.openBL();
	this.appendDescription("edited on " + formatter.format(date) + " by using eUniverse");
	this.saveBL();


    parentLogger.log(Level.INFO,String.format("Publish business layer into \"%s/%s.unx\"", CmsResourceService.UNIVERSES_ROOT,businessLayer.getName()));
    cmsResourceService.publish(blPath, CmsResourceService.UNIVERSES_ROOT, true);


}


 String getAlphaNumericString(int n) 
{ 

    // lower limit for LowerCase Letters 
    int lowerLimit = 97; 

    // lower limit for LowerCase Letters 
    int upperLimit = 122; 

    Random random = new Random(); 

    // Create a StringBuffer to store the result 
    StringBuffer r = new StringBuffer(n); 

    for (int i = 0; i < n; i++) { 

        // take a random value between 97 and 122 
        int nextRandomChar = lowerLimit 
                             + (int)(random.nextFloat() 
                                     * (upperLimit - lowerLimit + 1)); 

        // append a character at the end of bs 
        r.append((char)nextRandomChar); 
    } 

    // return the resultant string 
    return r.toString(); 
} 


}
