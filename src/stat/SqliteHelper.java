package stat;

import java.sql.*;

public class SqliteHelper {

	public static Connection getConnection(String dbPath) {
		try {
			Class.forName("org.sqlite.JDBC");
			Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
			System.out.println("Opened database successfully " + dbPath);
			return connection;
		} catch (Exception e) {
			System.err.println("Open database failed");
			e.printStackTrace();
		}
		return null;
	}
	
	public static void silentlyClose(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				System.err.println("Close failed");
			}
		}
	}
	
	// TODO: prepare statement
	public static boolean executeUpdate(Connection connection, String sql) {
		if (sql == null || sql.length() <= 0) {
		    System.out.println("Invalid input sql!");
			return false;
		}
		
		if (connection == null) {
		    System.out.println("Cannot get database connection");
		    return false;
		}
		
		try {
			Statement statement = connection.createStatement();
		    System.out.println("Create statement successfully");
			statement.executeUpdate(sql);
			statement.close();
			return true;
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
		return false;
	}

//
//	public static void main(String[] args) {
//		boolean result = SqliteHelper.executeUpdate("Create table active_users ("
//				+ "_id int primary key not null,"
//				+ "date int not null,"
//				+ "uuid text not null,"
//				+ "log_count int)");
//		System.out.println("Create table result:" + result);
//	}

}
