package com.github.s262316.dbtools.tableexport;

public class ColumnDescr
{
	private String name;
	// from java.sql.Types
	private int sqlType;

	public ColumnDescr(String name, int sqlType)
	{
		this.name = name;
		this.sqlType=sqlType;
	}

	public String getName()
	{
		return name;
	}

	public int getSqlType()
	{
		return sqlType;
	}
}