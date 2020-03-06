package com.bwise.eUniverse;

public class CJoin {

	private String leftTable;
	private String leftColumn;
	private String rightTable;
	private String rightColumn;
	private String sql;
	
	CJoin() {
		
	}
	
	CJoin(String pleftTable, String pleftColumn, String prightTable, String prightColumn) {
		leftTable=pleftTable;
		leftColumn=pleftColumn;
		rightTable=prightTable;
		rightColumn=prightColumn;
	}
	
	CJoin(String psql) {
		sql = psql;
	}
	
	
	String getSQLJoin() {
		if (sql == null) {
		String joinsql = leftTable+"."+leftColumn + " = " + rightTable + "."+rightColumn;

		return joinsql;
		}
		else 
			return sql;
	}
	void setLeftTable(String pleftTable) {
		leftTable = pleftTable;
	}
	String getLeftTable() {
		return leftTable;
	}
	
	void setRightTable(String prightTable) {
		rightTable = prightTable;
	}
	String getRightTable() {
		return rightTable;
	}
	
	void setLeftColumn(String pleftColumn) {
		leftColumn = pleftColumn;
	}
	String getLeftColumn() {
		return leftColumn;
	}
	
	void setRightColumn(String prightColumn) {
		rightColumn = prightColumn;
	}
	String getRightColumn() {
		return rightColumn;
	}
	
	@Override
	public int hashCode() {
	    return this.getSQLJoin().hashCode();
	}
	@Override
    public boolean equals(Object o) {
		// If the object is compared with itself then return true   
        if (o == this) { 
            return true; 
        } 
        
        /* Check if o is an instance of Complex or not 
        "null instanceof [type]" also returns false */
      if (!(o instanceof CJoin)) { 
          return false; 
      } 
      
   // typecast o to Complex so that we can compare data members  
      CJoin c = (CJoin) o; 
      
      return this.getSQLJoin().equals(c.getSQLJoin()); 
		
	}

}
