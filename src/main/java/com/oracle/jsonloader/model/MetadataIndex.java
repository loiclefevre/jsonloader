package com.oracle.jsonloader.model;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import static com.oracle.jsonloader.util.Console.println;

public class MetadataIndex {
    private String name;
    private boolean unique;
    private MetadataKey key;

    public MetadataIndex() {
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name.replace('.', '_');
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public MetadataKey getKey() {
        return key;
    }

    public void setKey(MetadataKey key) {
        this.key = key;
    }

    public void createIndex(String collectionName, PoolDataSource pds) throws Exception {
        if (pds != null) {
            final Properties props = new Properties();
            props.put("oracle.soda.sharedMetadataCache", "true");
            props.put("oracle.soda.localMetadataCache", "true");

            final OracleRDBMSClient cl = new OracleRDBMSClient(props);

            try (Connection c = pds.getConnection()) {
                final OracleDatabase db = cl.getDatabase(c);
                final OracleCollection oracleCollection = db.openCollection(collectionName);

                if (!name.contains("$**")) {
                    final String indexSpec = key.spatial ?
                            String.format("{\"name\": \"%s\", \"spatial\": \"%s\"}", collectionName + "$" + name, key.columns.get(0).name)
                            :
                            String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + name, getCreateIndexColumns(), unique);

                    try {
                        final long start = System.currentTimeMillis();
                        // System.out.println("\n"+indexSpec);

                        oracleCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
                        final long end = System.currentTimeMillis();

                        println(" OK (" + (end - start) + " ms)");
                    } catch (OracleException oe) {
                        if (oe.getErrorCode() == 2053) {
                            //oe.printStackTrace();
                            println(" already exists!");
                        } else {
                            println(String.format(" ERROR (%d: %s): %s", oe.getErrorCode(), oe.getMessage(), indexSpec));
                            //throw oe;
                        }
                        //System.out.println(oe.getErrorCode()+" "+oe.getMessage());
                    } catch (Exception e) {
                        //System.out.println("ARGH "+e.getClass().getName());
                        //e.printStackTrace();
                        throw e;
                    }
                }
            }
        } else {
            if (!name.contains("$**")) {
                final String indexSpec = key.spatial ?
                        String.format("{\"name\": \"%s\", \"spatial\": \"%s\"}", collectionName + "$" + name, key.columns.get(0).name)
                        :
                        String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + name, getCreateIndexColumns(), unique);

                try {
                    final long start = System.currentTimeMillis();
                    println("\n" + indexSpec);
                    final long end = System.currentTimeMillis();

                    println(" OK (" + (end - start) + " ms)");
                } catch (Exception e) {
                    //e.printStackTrace();
                    throw e;
                }
            }
        }
    }

    public static void createJSONSearchIndex(String collectionName, PoolDataSource pds) throws Exception {
//        final Properties props = new Properties();
//        props.put("oracle.soda.sharedMetadataCache", "true");
//        props.put("oracle.soda.localMetadataCache", "true");
//
//        final OracleRDBMSClient cl = new OracleRDBMSClient(props);
//
//        try (Connection c = pds.getConnection()) {
//            final OracleDatabase db = cl.getDatabase(c);
//            final OracleCollection oracleCollection = db.openCollection(collectionName);
//
//            final String indexSpec = String.format("{\"name\": \"%s\", \"dataguide\": \"off\"}", collectionName+"_search_index" );
//
//            try {
//                final long start = System.currentTimeMillis();
//                oracleCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
//                final long end = System.currentTimeMillis();
//
//                System.out.println(" OK ("+(end - start)+" ms)" /*+ indexSpec*/);
//            } catch (OracleException oe) {
//                if (oe.getErrorCode() == 2053) {
//                    System.out.println(" already exists!");
//                } else {
//                    throw oe;
//                }
//                //System.out.println(oe.getErrorCode()+" "+oe.getMessage());
//            } catch (Exception e) {
//                //e.printStackTrace();
//                throw e;
//            }
//
//        }

        try (Connection c = pds.getConnection()) {
            try (Statement s = c.createStatement()) {
                //final String indexSpec = String.format("{\"name\": \"%s\", \"dataguide\": \"off\"}", collectionName+"_search_index" );

                try {
                    final long start = System.currentTimeMillis();
                    s.execute(String.format("CREATE SEARCH INDEX %s$search_index ON %s (json_document) FOR JSON " +
                            "PARAMETERS('DATAGUIDE OFF SYNC(MANUAL)')", collectionName, collectionName));
//                    s.execute(String.format("CREATE SEARCH INDEX %s_search_index ON %s (json_document) FOR JSON " +
//                            "PARAMETERS('DATAGUIDE OFF SYNC(every \"freq=secondly;interval=10\" MEMORY 2G parallel 6)')",collectionName,collectionName));
                    final long end = System.currentTimeMillis();

                    System.out.println(" OK (" + (end - start) + " ms)" /*+ indexSpec*/);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }
    }

    private String getCreateIndexColumns() {
        final StringBuilder s = new StringBuilder();

        for (IndexColumn ic : key.columns) {
            if (s.length() > 0) {
                s.append(", ");
            }

            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
        }

        return s.toString();
    }
}
