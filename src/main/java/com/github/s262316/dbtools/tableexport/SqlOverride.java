package com.github.s262316.dbtools.tableexport;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("sql")
public class SqlOverride
{
	private Map<String, String> override=new HashMap<>();

	public Map<String, String> getOverride()
	{
		return override;
	}

	public void setOverride(Map<String, String> override)
	{
		this.override = override;
	}
	
	public String sqlFor(String tableName)
	{
		return override.getOrDefault(tableName, "select * from "+tableName);
	}
}
