package com.github.s262316.dbtools.tableorder;

import java.util.ArrayList;
import java.util.List;

public class Table
{
	List<Table> dependentTables=new ArrayList<>();
	String tableName;
	
	public Table(String tableName)
	{
		this.tableName=tableName;
	}
	
	public Table(String tableName, List<Table> dependentTables)
	{
		this.tableName=tableName;
		this.dependentTables=dependentTables;
	}
	
	public Table merge(List<Table> newDependentTables)
	{
		dependentTables.addAll(newDependentTables);
		return this;
	}

	public List<Table> getDependentTables()
	{
		return dependentTables;
	}

	public String getTableName()
	{
		return tableName;
	}
}