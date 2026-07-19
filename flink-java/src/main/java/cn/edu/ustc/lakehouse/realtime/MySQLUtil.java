package cn.edu.ustc.lakehouse.realtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

public final class MySQLUtil {
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public MySQLUtil(RealtimeJobConfig config) {
        this.jdbcUrl = config.getDimensionJdbcUrl();
        this.username = config.getDimensionUsername();
        this.password = config.getDimensionPassword();
    }

    public Map<String, Object> findByKey(String tableName, String key) {
        requireSafeIdentifier(tableName);
        String keyColumn = tableName + "_id";
        String sql = "SELECT * FROM `" + tableName + "` WHERE `" + keyColumn + "` = ?";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? readRow(resultSet) : Collections.emptyMap();
            }
        } catch (Exception queryError) {
            return Collections.emptyMap();
        }
    }

    private static Map<String, Object> readRow(ResultSet resultSet) throws Exception {
        ResultSetMetaData metadata = resultSet.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            row.put(metadata.getColumnLabel(index), resultSet.getObject(index));
        }
        return row;
    }

    private static void requireSafeIdentifier(String identifier) {
        if (identifier == null || !SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Unsafe MySQL identifier: " + identifier);
        }
    }
}
