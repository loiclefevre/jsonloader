package com.oracle.jsonloader.model;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.oracle.jsonloader.util.Console.println;

public class MetadataIndex {

    public static int INDEXED_FIELD_MAX_LENGTH_WARNING = 100;
    private static Logger log = LoggerFactory.getLogger("IndexBuilder");

    private String name;
    private boolean unique;
    private MetadataKey key;
    private boolean warning;

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

    public void createIndex(String collectionName, PoolDataSource pds, final Map<String, Integer> maxLengths, final Map<String, Set<String>> fieldsDataTypes, Set<String> cantIndex, Properties oracleMetadata) throws Exception {
        if (pds != null) {
            final Properties props = new Properties();
            props.put("oracle.soda.sharedMetadataCache", "true");
            props.put("oracle.soda.localMetadataCache", "true");

            final OracleRDBMSClient cl = new OracleRDBMSClient(props);

            try (Connection c = pds.getConnection()) {
                if (key.spatial) {
                    final String SQLStatement = String.format(
                            "create index %s on %s " +
                                    "(JSON_VALUE(JSON_DOCUMENT, '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
                                    "indextype is MDSYS.SPATIAL_INDEX parallel 8", collectionName + "$" + name, collectionName.toUpperCase(), key.columns.get(0).name);

                    try (Statement s = c.createStatement()) {
                        // String.format("{\"name\": \"%s\", \"spatial\": \"%s\"}", collectionName + "$" + name, key.columns.get(0).name)
                        final long start = System.currentTimeMillis();

                        s.execute(SQLStatement);

                        final long end = System.currentTimeMillis();

                        println(" OK "+(warning?"with warning(s) ":"")+"(" + (end - start) + " ms)");
                    } catch (SQLException e) {
                        /*if (e.getErrorCode() == 2053) {
                            //oe.printStackTrace();
                            println(" already exists!");
                        } else { */
                            println(String.format(" ERROR (%d: %s): %s", e.getErrorCode(), e.getMessage(), SQLStatement));
                            log.error(String.format(" ERROR (%d: %s): %s", e.getErrorCode(), e.getMessage(), SQLStatement),e);
                            //oe.printStackTrace();
                            //throw oe;
                    }

                } else {
                    final OracleDatabase db = cl.getDatabase(c);
                    final OracleCollection oracleCollection = db.openCollection(collectionName.toUpperCase());

                    if (!name.contains("$**")) {
                        final String indexSpec = key.spatial ?
                                String.format("{\"name\": \"%s\", \"spatial\": \"%s\"}", collectionName + "$" + name, key.columns.get(0).name)
                                :
                                String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + name, getCreateIndexColumns(maxLengths,fieldsDataTypes,cantIndex,oracleMetadata, collectionName), unique);

                        try {
                            final long start = System.currentTimeMillis();
                            //System.out.println("\n"+indexSpec);

                            oracleCollection.admin().createIndex(db.createDocumentFromString(indexSpec));
                            final long end = System.currentTimeMillis();

                            println(" OK "+(warning?"with warning(s) ":"")+"(" + (end - start) + " ms)");
                        } catch (OracleException oe) {
                            if (oe.getErrorCode() == 2053) {
                                //oe.printStackTrace();
                                println(" already exists!");
                            } else {
                                if(oe.getCause() != null && oe.getCause() instanceof  SQLException ) {
                                    switch(((SQLException)oe.getCause()).getErrorCode()) {
                                        case 1408:
                                            println(" already exists! (array column removed)");
                                            break;

                                        case 40470:
                                            println(" array found, multi-value index not supported yet!");
                                            log.error(String.format(" ERROR (%d: %s): %s, array found, multi-value index not supported yet!", oe.getErrorCode(), oe.getMessage(), indexSpec), oe);
                                            break;

                                        default:
                                            println(String.format(" ERROR (%d: %s): %s", oe.getErrorCode(), oe.getMessage(), indexSpec));
                                            log.error(String.format(" ERROR (%d: %s): %s", oe.getErrorCode(), oe.getMessage(), indexSpec), oe);
                                            break;
                                    }
                                } else {
                                    println(String.format(" ERROR (%d: %s): %s", oe.getErrorCode(), oe.getMessage(), indexSpec));
                                    log.error(String.format(" ERROR (%d: %s): %s", oe.getErrorCode(), oe.getMessage(), indexSpec), oe);
                                    //throw oe;
                                }
                            }
                            //System.out.println(oe.getErrorCode()+" "+oe.getMessage());
                        } catch (Exception e) {
                            //System.out.println("ARGH "+e.getClass().getName());
                            //e.printStackTrace();
                            log.error("Creating index "+name+" ("+indexSpec+")",e);
                            throw e;
                        }
                    }
                }
            }
        } else {
            if (key.spatial) {
                final String SQLStatement = String.format(
                        "create index %s on %s " +
                                "(JSON_VALUE(JSON_DOCUMENT, '$.%s' returning SDO_GEOMETRY ERROR ON ERROR NULL ON EMPTY)) " +
                                "indextype is MDSYS.SPATIAL_INDEX parallel 8", collectionName + "$" + name, collectionName.toUpperCase(), key.columns.get(0).name);

                try {
                    final long start = System.currentTimeMillis();
                    println("\n" + SQLStatement);
                    final long end = System.currentTimeMillis();

                    println(" OK "+(warning?"with warning(s) ":"")+"(" + (end - start) + " ms)");
                } catch (Exception e) {
                    //e.printStackTrace();
                    throw e;
                }
            }
            else
            if (!name.contains("$**")) {
                final String indexSpec = key.spatial ?
                        String.format("{\"name\": \"%s\", \"spatial\": \"%s\"}", collectionName + "$" + name, key.columns.get(0).name)
                        :
                        String.format("{\"name\": \"%s\", \"fields\": [%s], \"unique\": %s}", collectionName + "$" + name, getCreateIndexColumns(maxLengths, fieldsDataTypes,cantIndex, oracleMetadata,collectionName), unique);

                try {
                    final long start = System.currentTimeMillis();
                    println("\n" + indexSpec);
                    final long end = System.currentTimeMillis();

                    println(" OK "+(warning?"with warning(s) ":"")+"(" + (end - start) + " ms)");
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
                            "PARAMETERS('DATAGUIDE OFF SYNC(MANUAL)')", collectionName, collectionName.toUpperCase()));
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

    /**
     * @see https://docs.oracle.com/en/database/oracle/simple-oracle-document-access/adsdi/soda-index-specifications-reference.html#GUID-00C06941-6FFD-4CEB-81B6-9A7FBD577A2C
     * @param maxLengths
     * @param fieldsDataTypes
     * @return
     */
    private String getCreateIndexColumns(final Map<String, Integer> maxLengths, final Map<String, Set<String>> fieldsDataTypes, Set<String> cantIndex, Properties oracleMetadata, String collectionName) {
        final StringBuilder s = new StringBuilder();

        for (IndexColumn ic : key.columns) {
            if (s.length() > 0) {
                    s.append(", ");
            }

            if(cantIndex.contains(ic.name)) {
                if (s.length() > 0) {
                    s.setLength(s.length() - 2);
                }

                log.warn("For collection "+collectionName+", the field "+ic.name+" has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which path belongs to an array");
                warning = true;
            }
            else
            if(oracleMetadata.containsKey(collectionName+"."+ic.name+".datatype")) {
               final String dataType =  (String)oracleMetadata.get(collectionName+"."+ic.name+".datatype");

               switch(dataType) {
                   case "number":
                       s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                       break;

                   case "string":
                       final String dataLength =  (String)oracleMetadata.get(collectionName+"."+ic.name+".maxlength");

                       if(Integer.parseInt(dataLength) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                           log.warn("For collection "+collectionName+", index "+name+" has a string field \""+ic.name+"\" with a maxlength ("+dataLength+" bytes, defined in oracle metadata file) strictly larger than the threshold "+INDEXED_FIELD_MAX_LENGTH_WARNING);
                           warning = true;
                       }

                       s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(dataLength).append("}");
                       break;

                   default:
                       s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                       break;
               }
            } else {
                if (fieldsDataTypes.containsKey(ic.name)) {
                    if (fieldsDataTypes.get(ic.name).size() == 1) {
                        final String dataType = fieldsDataTypes.get(ic.name).iterator().next();
                        if ("number".equals(dataType)) {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        } else if ("array".equals(dataType)) {
                            // remove the trailing ", "
                            if (s.length() > 0) {
                                s.setLength(s.length() - 2);
                            }

                            log.warn("For collection "+collectionName+", the field "+ic.name+" has been removed from the index definition: Multi-value index not yet supported for index " + name + " on field " + ic.name + " which is an array");
                            warning = true;
                            //s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"number\"}");
                        } else {
                            if (maxLengths.containsKey(ic.name)) {
                                if(maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                    log.warn("For collection "+collectionName+", index "+name+" has a string field \""+ic.name+"\" with a maxlength ("+maxLengths.get(ic.name)+" bytes) strictly larger than the threshold "+INDEXED_FIELD_MAX_LENGTH_WARNING);
                                    warning = true;
                                }
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                            } else {
                                s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                            }
                        }
                    } else {
                        if (maxLengths.containsKey(ic.name)) {
                            if(maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                                log.warn("For collection "+collectionName+", index "+name+" has a string field \""+ic.name+"\" with a maxlength ("+maxLengths.get(ic.name)+" bytes) strictly larger than the threshold "+INDEXED_FIELD_MAX_LENGTH_WARNING);
                                warning = true;
                            }
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                        } else {
                            s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"datatype\": \"string\"}");
                        }
                    }
                } else if (maxLengths.containsKey(ic.name)) {
                    if(maxLengths.get(ic.name) > INDEXED_FIELD_MAX_LENGTH_WARNING) {
                        log.warn("For collection "+collectionName+", index "+name+" has a field \""+ic.name+"\" with a maxlength ("+maxLengths.get(ic.name)+" bytes) strictly larger than the threshold "+INDEXED_FIELD_MAX_LENGTH_WARNING);
                        warning = true;
                    }
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\", \"maxlength\": ").append(maxLengths.get(ic.name)).append("}");
                } else {
                    s.append("{\"path\": \"").append(ic.name).append("\", \"order\": \"").append(ic.asc ? "asc" : "desc").append("\"}");
                }
            }
        }

        return s.toString();
    }
}
