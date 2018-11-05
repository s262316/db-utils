package com.github.s262316.dbtools.tableexport;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

public class Formatting
{
	private static SimpleDateFormat sqlInsertDateFormat=new SimpleDateFormat("dd-MMM-yyyy");
	private static SimpleDateFormat sqlInsertTimestampFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat oracleCsvTimestampFormat=new SimpleDateFormat("dd-MMM-yyyy HH.mm.ss.SSSSS");
	
	public static String format(Date date)
	{
		if(date==null)
			return "NULL";
		else
			return "'"+sqlInsertDateFormat.format(date)+"'";
	}
	
	public static String formatTimestamp(Date date)
	{
		if(date==null)
			return "NULL";
		else
			return "TIMESTAMP '"+sqlInsertTimestampFormat.format(date)+"'";
	}
	
	public static String formatDateForCsv(Timestamp date)
	{
		if(date==null)
			return "";
		else
			return sqlInsertDateFormat.format(date);
	}
	
	public static String formatTimestampForCsv(Timestamp date)
	{
		if(date==null)
			return "";
		else
			return oracleCsvTimestampFormat.format(date);
	}
	
	public static String format(String str)
	{
		if(str==null)
			return "NULL";
		else
		{
			str=StringUtils.replace(str, "'", "''");
			str=StringUtils.replace(str, "\\", "\\\\");
			return "'"+str+"'";
		}
	}
	
	public static String formatforCsv(String str)
	{
		if(str==null)
			return "";
		else
		{
			return str;
		}
	}
	
	public static String formatInt(ResultSet rs, int num) throws SQLException
	{
		if(rs.wasNull())
			return "NULL";
		else
			return String.valueOf(num);
	}
	
	public static String formatInt(BigDecimal num)
	{
		if(num==null)
			return "";
		else
			return num.toPlainString();
	}	
	
	public static String formatLong(ResultSet rs, long num) throws SQLException
	{
		if(rs.wasNull())
			return "NULL";
		else
			return String.valueOf(num);
	}	

	public static String formatBigDecimal(ResultSet rs, BigDecimal num) throws SQLException
	{
		if(rs.wasNull())
			return "NULL";
		else
			return num.toPlainString();
	}
}
