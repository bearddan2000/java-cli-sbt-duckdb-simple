package example;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class Main {
    private static Connection conn = null;
    private static Statement stmt = null;

    // Utility function
    private static String removeSlashFromPathIfWindows(String path) {
        if (System.getProperty("os.name")
                .toLowerCase()
                .contains("win")) {
            return path.replaceFirst("/", "");
        }
        return path;
    }

    private static String getResourceAbsolutePath(String name) {
        return Optional.ofNullable(Main.class
                        .getResource(name))
                .map(URL::getPath)
                .map(Main::removeSlashFromPathIfWindows)
                .orElseThrow(IllegalArgumentException::new);
    }

    private static void setupOnce() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
    }

    private static void setup() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        stmt = conn.createStatement();
    }

    private static void tearDown() throws SQLException {
        stmt.close();
        conn.close();
    }
    
    private static void readCsv() throws SQLException {
        String filePath = getResourceAbsolutePath("/dog.csv");
        String query = String.format("SELECT * FROM read_csv('%s')", filePath);

        setup();

        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData metadata = rs.getMetaData();

        for (int n=1; n<=metadata.getColumnCount(); n++) {
            String header = metadata.getColumnLabel(n);
            System.out.println(header);
        }

        tearDown();
    }

    private static void readJson() throws SQLException {
        String filePath = getResourceAbsolutePath("/dog.json");
        String query = String.format("SELECT * FROM read_json('%s')", filePath);

        setup();

        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData metadata = rs.getMetaData();

        for (int n=1; n<=metadata.getColumnCount(); n++) {
            String header = metadata.getColumnLabel(n);
            System.out.println(header);
        }

        tearDown();
    }

	public static void main(String[] args) {
        try {
            setupOnce();
            readCsv();
            readJson();
        }
        catch(Exception e){}
	}
}
