package com.github.igorsuhorukov.arrow.osm.example;

import java.sql.*;

public class CalculatePlaceOfWorshipBuilding {
    public static void main(String[] args) throws Exception {
        if(args.length!=1) throw new IllegalArgumentException("Specify source dataset path for parquet files");
        String sql = "SELECT count(*) FROM parquet_scan('"+args[0]+"') where closed and " +
                "(tags['building'][1] in ('church','cathedral','chapel','mosque','synagogue','temple','font')" +
                " or (tags['building'][1] is not null and " +
                         "(tags['amenity'][1]='place_of_worship' or tags['religion'][1] is not null)))";
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement pragmaStatement = connection.createStatement();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            //preparedStatement.setObject(1, args[0]); issue with JDBC driver. Query return: Not implemented type: UNKNOWN
            pragmaStatement.execute("PRAGMA enable_profiling");
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()){
                    System.out.println(resultSet.getLong(1));
                }
            }
        }
    }
}
