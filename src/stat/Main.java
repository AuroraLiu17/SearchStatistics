package stat;

import java.sql.Connection;
import java.sql.Statement;

import stat.DataContract.DataDBInfo;

public class Main {

	private static final String META_TABLE_NAME = "meta_data";
	private static final String INSERT_FORMAT = "INSERT INTO " + META_TABLE_NAME + " (" 
			+ DataDBInfo.DATA_TYPE + "," + DataDBInfo.DATE + "," + DataDBInfo.DB_NAME + "," + DataDBInfo.TABLE_NAME
			+ ") VALUES (%d, '%s', '%s', '%s')";
	
	public static void main(String[] args) {
		Connection connection = SqliteHelper.getConnection(StorageHelper.DATA_DB_INFO_DB_PATH);
		if (connection == null) {
			System.err.println("Nothing we can do if data db cannot be connected");
			return;
		}

		if (!DataDBHelper.doTableExist(connection, "meta_data")) {
			boolean result = SqliteHelper.executeUpdate(connection,
					"CREATE TABLE " + "meta_data" + " ("
			+ DataDBInfo.ID + " integer PRIMARY KEY,"
			+ DataDBInfo.DATA_TYPE + " integer not null,"
			+ DataDBInfo.DATE + " text not null,"
			+ DataDBInfo.DB_NAME + " text not null,"
			+ DataDBInfo.TABLE_NAME + " text not null" + ")");
			if (!result) {
				System.err.println("Nothing we can do if meta data table cannot be created");
				return;
			}
		}
		
		new ActiveUserReader().readAll();
	}
	
	public static boolean onRecordTable(int dataType, String date, String dbName, String tableName) {
		Connection connection = SqliteHelper.getConnection(StorageHelper.DATA_DB_INFO_DB_PATH);
		if (connection == null) {
			System.err.println("Nothing we can do if data db cannot be connected");
			return false;
		}
		try {
			Statement statement = connection.createStatement();
			String sql = String.format(INSERT_FORMAT, dataType, date, dbName, tableName);
			int result = statement.executeUpdate(sql);
			System.out.println(String.format("%d record row (%d, %s, %s, %s) ", result, dataType, date, dbName, tableName));
			return result > 0;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

}
