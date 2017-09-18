package stat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Calendar;

public class DataDBHelper {
	
	public static String getDBNameForDataType(int dataType) {
		switch (dataType) {
		case DataContract.DataDBInfo.DATA_TYPE_ACTIVE_USER:
			return "active_user";
		case DataContract.DataDBInfo.DATA_TYPE_NEW_USER:
			return "new_user";
		case DataContract.DataDBInfo.DATA_TYPE_QUERY:
			return "query";
		default:
			throw new IllegalArgumentException("Unexpected data type " + dataType);
		}
	}
	
	public static String getTableNameFor(int dataType, String date) {
		switch (dataType) {
		case DataContract.DataDBInfo.DATA_TYPE_ACTIVE_USER:
		case DataContract.DataDBInfo.DATA_TYPE_NEW_USER:
			return "T" + date;
		case DataContract.DataDBInfo.DATA_TYPE_QUERY:
			// Weekly based
			Calendar calendar = intDateToCalendar(date);
			return String.format("T%d%d", calendar.get(Calendar.YEAR), calendar.get(Calendar.WEEK_OF_YEAR));
		default:
			throw new IllegalArgumentException("Unexpected data type " + dataType);
		}
	}
	
	private static final String SQL_TABLE_EXISTANCE = 
			"select * from sqlite_master where type='table' and name = '%s'";  
	
	public static Connection getDBConnectionForDataType(int dataType) {
		String dbName = DataDBHelper.getDBNameForDataType(dataType);
		return SqliteHelper.getConnection(StorageHelper.DB_FOLDER_PATH + dbName + ".db");
	}
	
	public static boolean doTableExist(Connection connection, String tableName) {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(String.format(SQL_TABLE_EXISTANCE, tableName));
			return resultSet != null && resultSet.next();
		} catch (Exception e) {
			System.err.println("Failed check table existance");
			e.printStackTrace();
		} finally {
			SqliteHelper.silentlyClose(statement);
		}
		return false;
	}
	
	public static Calendar intDateToCalendar(String date) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.valueOf(date.substring(0, 4)));
		calendar.set(Calendar.MONTH, Integer.valueOf(date.substring(4, 6)) - 1);
		calendar.set(Calendar.DAY_OF_MONTH, Integer.valueOf(date.substring(6, 8)));
		return calendar;
	}
}
