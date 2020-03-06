package com.bwise.eUniverse;

import com.sap.sl.sdk.authoring.businesslayer.DataType;


public class CDimension {
	String name;
	String select;
	String mapping;
	String description;
	DataType type;
	String bwisetype;
	String parent;
	String bwiseclass;
	
	CDimension(String pname, String pmapping, String pselect, String ptype, String pclass) {
		name = pname;
		mapping = pmapping;
		select = exceptionsFix(pselect);
		bwisetype=ptype;
		bwiseclass = pclass;
		
		
		 type = DataType.STRING;
		switch (bwisetype) {
		case "TEXT":
			type = DataType.LONG_TEXT;
			break;
		case "MONETARY_AMOUNT":
			type = DataType.NUMERIC;
			// Amount value are stored in another column with _AMOUNT at the end
			select += "_AMOUNT";
			break;
		case "BOOLEAN":
			type = DataType.NUMERIC;
			break;
		case "DATE":
			type = DataType.DATE;
			break;
		case "DATETIME":
			type = DataType.DATE_TIME;
			break;
		case "FLOAT":
			type = DataType.NUMERIC;
			break;
		case "AREA_AMOUNT":
			type = DataType.NUMERIC;
			break;
		case "INTEGER":
			type = DataType.NUMERIC;
			break;
		default:
		

			}
	}
	
	String exceptionsFix(String sql) {
		
		
		if (sql.endsWith("ASSESSMENTDEFINITIONIDNAME"))
			sql = sql.replace("ASSESSMENTDEFINITIONIDNAME", "ASSESSMENTDEFINITIONNAME");
		
		if (sql.endsWith("CONTROLMONITORINGASSESSMENTIDNAME"))
			sql = sql.replace("CONTROLMONITORINGASSESSMENTIDNAME", "CONTROLMONITORINGASSESSMNTNAME");

		
		return sql;
	}
	
	void setFolder(String pparent) {
		parent = pparent;
	}
	
	public void setBWiseClass(String pclass) {
		bwiseclass = pclass;
	}
	public String getBWiseClass() {
		return bwiseclass;
	}
	
public String getDesc() {
	return description;
}
public void setDesc(String pdesc) {
	this.description = pdesc;
}
	public String getBWiseType() {
		return this.bwisetype;
	}
	String getName() {
		return name;
	}
	
	String getSelect() {
		return select;
	}
	
	String getMapping() {
		return mapping;
	}
	void setMapping(String pmapping) {
		mapping = pmapping;
	}
	
	DataType getType() {
		return type;
	}
	
	void setType(DataType ptype) {
		type=ptype;
	}
	@Override
    public boolean equals(Object o) {
		// If the object is compared with itself then return true   
        if (o == this) { 
            return true; 
        } 
        
        /* Check if o is an instance of Complex or not 
        "null instanceof [type]" also returns false */
      if (!(o instanceof CDimension)) { 
          return false; 
      } 
      
   // typecast o to Complex so that we can compare data members  
      CDimension c = (CDimension) o; 
      
      return this.mapping.equals(c.mapping) && this.name.equals(c.name); 
		
	}
}
