package stat;

public class DataContract {

	public interface ActiveUser {
		public static final String ID = "_id";
		public static final String DATE = "date";
		public static final String UUID = "uuid";
		public static final String LOG_COUNT = "log_count";
	}

	public interface NewUser {
		public static final String ID = "_id";
		public static final String DATE = "date";
		public static final String UUID = "uuid";
		public static final String REQUEST_COUNT = "request_count";
	}
	
	public interface QueryData {
		public static final String ID = "_id";
		public static final String DATE = "date";
		public static final String QUERY = "query";
		public static final String QUERY_COUNT = "query_count";
	}
	
	public interface DataDBInfo {
		public static final String ID = "_id";
		public static final String DATE = "date";
		public static final String DATA_TYPE = "data_type";
		public static final String DB_NAME = "db_name";
		public static final String TABLE_NAME = "table_name";
		
		public static final int DATA_TYPE_ACTIVE_USER = 1;
		public static final int DATA_TYPE_NEW_USER = 2;
		public static final int DATA_TYPE_QUERY = 3;
	}

}
