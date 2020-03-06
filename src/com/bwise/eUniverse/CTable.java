package com.bwise.eUniverse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sap.sl.sdk.authoring.businesslayer.DataType;

public class CTable {

	protected static final Logger parentLogger = LogManager.getLogger();
	
	private String name;
	private String name_raw;
	private String BWiseSQLname;
	private String SQLname;
	private String pk;
	private String fk;
	private String parentSQLJoin;
	private String SQLQuery;
	private String bwiseclass;
	private String bwiseclassOriginal;
	private String bwiseParent;
	private List<String> views;
	private String aLiasTable;
	private boolean isHistoric = false;
	private boolean isDimension = false;
	private boolean isCrossTable = false;
	private boolean isEnumTable = false;
	private boolean isMultiValue = false;
	private boolean isQuestionTable = false;
	private boolean isSQLView = false;
	private boolean noDimensions = false;
	
	private List<CTable> enumtables = null;
	
	CTable(String pname, String psql, String pclass) {
		name = util.makeName(pname, "");
		parentLogger.log(Level.TRACE,"setting name "+pname+" as " +name);
		bwiseclass = pclass;
		bwiseclassOriginal= pclass;
		views = new ArrayList<String>();
		
		BWiseSQLname = psql;
		
		
		Map.Entry<String, String> DWHMapEnum = util.findclosest(BWiseSQLname);
		
		pk = DWHMapEnum.getValue();
		SQLname = DWHMapEnum.getKey();

	}
	
	CTable(String pname, CTable p, String pbwiseclass) {
		name = util.makeName(pname,   "");
		parentLogger.log(Level.INFO,"setting name "+pname+" as " +name);
		bwiseclass = pbwiseclass;
		views = new ArrayList<String>();
		bwiseclassOriginal = pbwiseclass;
		BWiseSQLname = p.getBWiseSQLname();
		aLiasTable = p.getName();
		
		Map.Entry<String, String> DWHMapEnum = util.findclosest(BWiseSQLname);
		
		pk = DWHMapEnum.getValue();
		SQLname = DWHMapEnum.getKey();
		
		isSQLView = true;

	}
	
	public void setNoDimensions() {
		noDimensions = true;
	}
	public String getAliasTable() {
		return aLiasTable;
	}
	public boolean isAlias() {
		return isSQLView;
	}
	public void setNotAlias() {
		 isSQLView = false;
	}
	public void addToView(String pview) {
		if(!views.contains(pview))
			views.add(pview);
	}
	public List<String> getView() {
		return views;
	}
	public void setBWiseSQLname(String pBWiseSQLname) {
		BWiseSQLname = pBWiseSQLname;
	}
	public String getBWiseSQLname() {
		return BWiseSQLname;
	}
	public void setForeignKey(String pfk) {
		fk = pfk;
	}
	public String getForeignKey() {
		return fk;
	}
	public void setBWiseClass(String pclass) {
		bwiseclass = pclass;
	}
	public void setBWiseClassOriginal(String pclass) {
		bwiseclassOriginal = pclass;
	}
	public String getBWiseClass() {
		return bwiseclass;
	}
	public String getPrimaryKey() {
		return pk;
	}
	
	public boolean isCrossTable () {
		return isCrossTable;
	}
	
	public boolean isMultiValue () {
		return isMultiValue;
	}
	
	public void setSQLTable(String sqltable) {
		 this.SQLname = sqltable;
	}
	public String getSQLTable() {
		return SQLname;
	}
	
	public void setPrimaryKey(String ppk) {
		 pk = ppk;
	}
	public void setAsQuestionTable() {
		isDimension = false;
		isCrossTable = false;
		isEnumTable = false;
		isMultiValue = false;
		isQuestionTable = true;
	}
	
	public void setAsDimension() {
		isDimension = true;
		isCrossTable = false;
		isEnumTable = false;
		isMultiValue = false;
		isQuestionTable = false;
	}
	
	public void setAsMultiValue() {
		isDimension = false;
		isCrossTable = false;
		isEnumTable = false;
		isMultiValue = true;
		isQuestionTable = false;
	}
	
	public void setAsCrossTable() {
		isDimension = false;
		isCrossTable = true;
		isEnumTable = false;
		isMultiValue = false;
		isQuestionTable = false;
	}
	
	public void setAsEnumTable() {
		isDimension = false;
		isCrossTable = false;
		isEnumTable = true;
		isMultiValue = false;
		isQuestionTable = false;
	}
	
	public void enableHistoric() {
		isHistoric = true;
	}
	public void disableHistoric() {
		isHistoric = false;
	}
	
	public String getSQLQuery() {
		if (SQLQuery == null) {
			return this.getDefaultSQL();
		}
		return this.SQLQuery;
	}
	public void setSQLQuery(String sql) {
		SQLQuery = sql;
	}
	public String getDefaultSQL() {
		
		String WhereClause = "1=1";
		if (Main.isHistoricDWH && isHistoric) {
			if (Main.config.dbtype.equals("sqlserver"))
				WhereClause = "((CONVERT(VARCHAR(8), CAST(@Prompt(SD) AS DATE), 112)-19000000)*1000000000000+235959000000) BETWEEN T.VALIDFROM AND T.VALIDTO";
			else if (Main.config.dbtype.equals("oracle"))
				WhereClause = "'1' || TO_CHAR(TO_DATE(@Prompt(SD),'DD-MM-RRRR hh24:mi:ss') + 1  - INTERVAL '1' SECOND,'YYMMDDHH24MISS') || '000000' BETWEEN T.VALIDFROM AND T.VALIDTO";
		} else if (Main.isHistoricDWH)
			WhereClause = "T.ISLIVE=1";
		String sql ="SELECT T.* " + System.lineSeparator()
		+" FROM " + SQLname +" T "+ System.lineSeparator()
		+" WHERE " + WhereClause;
		return sql ;
	}
	
	public String getWhereClause() {
		
		String WhereClause = "1=1";
		if (Main.isHistoricDWH && isHistoric) {
			if (Main.config.dbtype.equals("sqlserver"))
				WhereClause = "((CONVERT(VARCHAR(8), CAST(@Prompt(SD) AS DATE), 112)-19000000)*1000000000000+235959000000) BETWEEN T.VALIDFROM AND T.VALIDTO";
			else if (Main.config.dbtype.equals("oracle"))
				WhereClause = "'1' || TO_CHAR(TO_DATE(@Prompt(SD),'DD-MM-RRRR hh24:mi:ss') + 1  - INTERVAL '1' SECOND,'YYMMDDHH24MISS') || '000000' BETWEEN T.VALIDFROM AND T.VALIDTO";
		} else if (Main.isHistoricDWH)
			WhereClause = "T.ISLIVE=1";

		return WhereClause ;
	}

	public void fetchEnumListTables() {
		
		enumtables = new ArrayList<CTable>();
		
		if(noDimensions)
			return ;
		
		if(!isMultiValue) {
		List<Map<String, String>>  enumlistables = Main.dm.getEnumList(BWiseSQLname);
		
		parentLogger.log(Level.INFO,"Found " + enumlistables.size() + " enumlist tables for " + BWiseSQLname);
		for (Map<String, String> enumtable : enumlistables) {
			CTable t = new CTable(name + "_" + enumtable.get("ATTRIBUTE"),enumtable.get("TABLENAME"), this.bwiseclass);
			//t.setNameRaw(name + "_" + enumtable.get("ATTRIBUTE"));
			t.setAsEnumTable();
			if(this.views.contains("ControlAssessment"))
				t.addToView("ControlAssessment");
			
			if(this.views.contains("OA"))
				t.addToView("OA");
			t.setForeignKey(enumtable.get("COLUMNNAME"));
			if(Main.config.histo && enumtable.get("ISHISTORIC").equals("1"))
				t.enableHistoric();
			if(!enumtables.contains(t)) {
				parentLogger.log(Level.INFO,"adding enumlist " + t.getName());
				enumtables.add(t);
			}
		}
		}
	}
	
	public List<CTable>  getEnumListTables() {
		return enumtables;
	}

	public List<CJoin>  getEnumListJoins() {
		
		 List<CJoin> joins= new ArrayList<CJoin>();
		 
		 for(CTable t : enumtables) {
			 
			 CJoin mainjoin = new CJoin(t.getName(),t.getPrimaryKey(), this.getName(), t.getForeignKey());
				
				joins.add(mainjoin);
		 }
		 
		return joins;
	}

	public List<CDimension> getDimensions(CTable maintable) {
		List<CDimension> dimensions = new ArrayList<CDimension>();
		
		if(noDimensions)
			return dimensions;

		
		List<Map<String, String>> columns = Main.dm.getColumns(this.BWiseSQLname, null, this.bwiseclassOriginal);
		
		
		for (Map<String, String> column : columns) {

			String sql = this.name + "." + column.get("COLUMNNAME");
			
				String mapping = this.bwiseclass + "." + column.get("ATTRIBUTE");
				
				//if (this.isCrossTable || this.isMultiValue)
				String type = column.get("TYPE");
				String dimname = column.get("LABEL");
				String folder =this.bwiseclass;
				if( maintable.isAlias() && (isCrossTable || isMultiValue))  {
					mapping = maintable.getName() + "_"+ mapping;
					folder =maintable.getName();

				}
			 if(!isMultiValue) {
				CDimension dim = new CDimension(dimname,  mapping, sql, type, this.bwiseclass);
				dim.setFolder(folder);
				
				//dim.setFolder(column.get("CLASSNAME"));
				
				if(!dimensions.contains(dim)) {
					parentLogger.log(Level.INFO,"adding dimension " + dim.getMapping());
					dimensions.add(dim);
				}
				
				if(dim.getBWiseType().equals("BOOLEAN")) {
					
					mapping += "_bool";
					dimname = column.get("LABEL")+" (Yes/No)";
					String sqlbool = "CASE WHEN "+sql+" = 1 THEN 'Yes' WHEN "+sql+" = 0 THEN 'No' END";
					
					CDimension dimbool = new CDimension(dimname, mapping, sqlbool, type, this.bwiseclass);
					dimbool.setFolder(folder);
					
					dimbool.setType(DataType.STRING);
					if(!dimensions.contains(dimbool)) {

						parentLogger.log(Level.INFO,"adding dimension " + dimbool.getMapping());
						dimensions.add(dimbool);
					}

				
				}
				
				// Identifier
				if(column.get("ROOTCLASSLINK").equals("EnumLiteral")) {
					mapping += "_identifier";
					dimname = column.get("LABEL")+" (Identifier)";
					sql = util.makeName(this.name +"_"+ column.get("ATTRIBUTE"), "") + ".IDENTIFIER";
					CDimension dimlink = new CDimension(dimname, mapping, sql, type, this.bwiseclass);
					dimlink.setFolder(folder);
					
					if(!dimensions.contains(dimlink)) {
						parentLogger.log(Level.INFO,"adding dimension " + dimlink.getMapping());
						dimensions.add(dimlink);
					}
				}
			 }
			 else if(isMultiValue) {
				
					mapping += "_mv";
					dimname = column.get("LABEL")+" (MultiValue)";
					sql = this.getName().replace("_SV", "_MV") +".MV";
					CDimension dimlink = new CDimension(dimname, mapping, sql, type, this.bwiseclass);
					dimlink.setFolder(folder);
					
					if(!dimensions.contains(dimlink)) {
						parentLogger.log(Level.INFO,"adding dimension " + dimlink.getMapping());
						dimensions.add(dimlink);
					}
					
				}
				

			}
				
			
		
		
		
		return dimensions;
		
	}
public List<CDimension> getNTableDimensions() {
		
		List<CDimension> dimensions = new ArrayList<CDimension>();
		
		for(int i = 1; i < 6; i++) {
		String sqlquestionmain = this.getName() +".LEVEL"+i+"NAME";
		CDimension dimsubject = new CDimension( "Level "+i+" Name", this.getName() + "_categLevel"+i+"Name", sqlquestionmain, "STRING", this.bwiseclass);
		dimsubject.setFolder(this.bwiseclass);
		dimensions.add(dimsubject);
		}

		return dimensions;
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
      if (!(o instanceof CTable)) { 
          return false; 
      } 
      
   // typecast o to Complex so that we can compare data members  
      CTable c = (CTable) o; 
      
      return this.name.equals(c.name); 
		
	}
	public String getName() {
		return name;
	}
	public void setName(String pname) {
		name = util.makeName(pname, "");
	}
	
	public String getparentSQLJoin() {
		return parentSQLJoin;
	}
	public void setparentSQLJoin(String pname) {
		parentSQLJoin = pname;
	}
	
	public String getNameRaw() {
		return name_raw;
	}
	public void setNameRaw(String pname) {
		name_raw = pname;
	}
	
	
	@Override
    public String toString() {
		return name;
	}
	
}
