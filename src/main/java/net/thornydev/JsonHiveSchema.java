package net.thornydev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generates Hive schemas for use with the JSON SerDe from
 * org.openx.data.jsonserde.JsonSerDe.  GitHub link: https://github.com/rcongiu/Hive-JSON-Serde
 *
 * Pass in a valid JSON document string to {@link JsonHiveSchema#createHiveSchema} and it will
 * return a Hive schema for the JSON document.
 *
 * It supports embedded JSON objects, arrays and the standard JSON scalar types: strings,
 * numbers, booleans and null.  You probably don't want null in the JSON document you provide
 * as Hive can't use that.  For numbers - if the example value has a decimal, it will be
 * typed as "double".  If the number has no decimal, it will be typed as "int".
 *
 * This program uses the JSON parsing code from json.org and that code is included in this
 * library, since it has not been packaged and made available for maven/ivy/gradle dependency
 * resolution.
 *
 * <strong>Use of main method:</strong> <br>
 *   JsonHiveSchema has a main method that takes a file path to a JSON doc - this file should have
 *   only one JSON file in it.  An optional second argument can be provided to name the Hive table
 *   that is generated.
 */
public class JsonHiveSchema  {

  // TODO: create cmd line args
  static void help() {
    System.out.println("Usage: Two arguments possible. First is required. Second is optional");
    System.out.println("  1st arg: path to JSON file to parse into Hive schema");
    System.out.println("  2nd arg (optional): tablename.  Defaults to 'x'");
  }

  public static void main( String[] args ) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("ERROR: No file specified");
    }

    if (args[0].equals("-h")) {
      help();
      System.exit(0);
    }

    JsonHiveSchema schemaWriter = null;

    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader( new FileReader(args[0]) );
    String line;
    while ( (line = br.readLine()) != null ) {
      sb.append(line).append("\n");
    }
    br.close();

    String tableName;
    Boolean isExternal;
    if (args.length == 5) {
      tableName = args[1];
      isExternal = Boolean.valueOf(args[2]);
      Map<String,String> partitions = JsonHiveSchema.parseStringToOrderedMap(args[3]);
      String location = args[4];
      schemaWriter = new JsonHiveSchema(tableName, isExternal, partitions, location);
    } else if (args.length == 4) {
      tableName = args[1];
      isExternal = Boolean.valueOf(args[2]);
      Map<String,String> partitions = JsonHiveSchema.parseStringToOrderedMap(args[3]);
      schemaWriter = new JsonHiveSchema(tableName, isExternal, partitions);
    } else if (args.length == 3) {
      tableName = args[1];
      isExternal = Boolean.valueOf(args[2]);
      schemaWriter = new JsonHiveSchema(tableName, isExternal);
    } else if (args.length == 2) {
      tableName = args[1];
      schemaWriter = new JsonHiveSchema(tableName);
    } else {
      schemaWriter = new JsonHiveSchema();
    }

    System.out.println(schemaWriter.createHiveSchema(sb.toString()));
  }


  private String tableName = "x";
  private Boolean isExternal = false;
  private Map<String,String> partitionDefinitions = new LinkedHashMap<String,String>(); // ordered
  private String location = "";


  public JsonHiveSchema() {}

  public JsonHiveSchema(String tableName) {
    this.tableName = tableName;
  }

  public JsonHiveSchema(String tableName, Boolean isExternal) {
    this.tableName = tableName;
    this.isExternal = isExternal;
  }

  public JsonHiveSchema(String tableName, Boolean isExternal, Map<String,String> partitionDefinitions) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.partitionDefinitions = partitionDefinitions;
  }

  public JsonHiveSchema(String tableName, Boolean isExternal, Map<String,String> partitionDefinitions, String location) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.partitionDefinitions = partitionDefinitions;
    this.location = location;
  }

  /**
   * Pass in any valid JSON object and a Hive schema will be returned for it.
   * You should avoid having null values in the JSON document, however.
   *
   * The Hive schema columns will be printed in alphabetical order - overall and
   * within subsections.
   *
   * @param json
   * @return string Hive schema
   * @throws JSONException if the JSON does not parse correctly
   */
  public String createHiveSchema(String json) throws JSONException {
    JSONObject jo = new JSONObject(json);

    String tableToken = isExternal ? "EXTERNAL TABLE " : "TABLE ";

    @SuppressWarnings("unchecked")
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("CREATE ")
        .append(tableToken)
        .append(tableName).append(" (\n");


    while (keys.hasNext()) {
      String k = keys.next();
      sb.append("  ");
      sb.append(k.toString());
      sb.append(' ');
      sb.append(valueToHiveSchema(jo.opt(k)));
      sb.append(',').append("\n");
    }
    sb.replace(sb.length() - 2, sb.length(), ")\n"); // remove last comma

    String partitionToken = this.partitionToken(partitionDefinitions);
    String locationToken = this.locationToken(location);

    return
        sb.append(partitionToken)
        .append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n")
        .append(locationToken)
        .append(";")
        .toString();
  }

  private String toHiveSchema(JSONObject o) throws JSONException {
    @SuppressWarnings("unchecked")
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("struct<");

    while (keys.hasNext()) {
      String k = keys.next();
      sb.append(k.toString());
      sb.append(':');
      sb.append(valueToHiveSchema(o.opt(k)));
      sb.append(", ");
    }
    sb.replace(sb.length() - 2, sb.length(), ">"); // remove last comma
    return sb.toString();
  }

  private String toHiveSchema(JSONArray a) throws JSONException {
    return "array<" + arrayJoin(a, ",") + '>';
  }

  private String arrayJoin(JSONArray a, String separator) throws JSONException {
    StringBuilder sb = new StringBuilder();

    if (a.length() == 0) {
      throw new IllegalStateException("Array is empty: " + a.toString());
    }

    Object entry0 = a.get(0);
    if ( isScalar(entry0) ) {
      sb.append( scalarType(entry0) );
    } else if (entry0 instanceof JSONObject) {
      sb.append( toHiveSchema((JSONObject)entry0) );
    } else if (entry0 instanceof JSONArray) {
      sb.append( toHiveSchema((JSONArray)entry0) );
    }
    return sb.toString();
  }

  private String scalarType(Object o) {
    if (o instanceof String) return "string";
    if (o instanceof Number) return scalarNumericType(o);
    if (o instanceof Boolean) return "boolean";
    return null;
  }

  private String scalarNumericType(Object o) {
    String s = o.toString();
    if (s.indexOf('.') > 0) {
      return "double";
    } else {
      return "int";
    }
  }

  private boolean isScalar(Object o) {
    return o instanceof String ||
        o instanceof Number ||
        o instanceof Boolean ||
        o == JSONObject.NULL;
  }

  private String valueToHiveSchema(Object o) throws JSONException {
    if ( isScalar(o) ) {
      return scalarType(o);
    } else if (o instanceof JSONObject) {
      return toHiveSchema((JSONObject)o);
    } else if (o instanceof JSONArray) {
      return toHiveSchema((JSONArray)o);
    } else {
      throw new IllegalArgumentException("unknown type: " + o.getClass());
    }
  }

  private String locationToken(String s) {
    if ( s.length() == 0 ) {
        return "";
    }
    StringBuilder sb = new StringBuilder("LOCATION '").append(s).append("'\n");
    return sb.toString();
  }

  // TODO : implement protected Hive types check and optional payload validation of partition fields.
  private String partitionToken(Map<String,String> m) {
    if ( m.size() == 0 ) {
        return "";
    }
    StringBuilder sb = new StringBuilder("PARTITIONED BY (");
    for ( Map.Entry<String, String> entry : m.entrySet() ) {
        sb.append(entry.getKey() + " " + entry.getValue())
          .append(", ");
    }
    sb.replace(sb.length() - 2, sb.length(), ")\n"); // remove last comma
    return sb.toString();
  }

  public static Map<String, String> parseStringToOrderedMap(String s) {
    try {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (String keyValue : s.split(",")) {
            String[] tuple2 = keyValue.split(":");
            if (tuple2.length != 2) {
                throw new RuntimeException("incorrect separator");
            }
            map.put(tuple2[0], tuple2[1]);
        }
        if (s.length() > 0 && map.size() == 0) {
            throw new RuntimeException("no separator found");
        }
        return map;
    } catch (Exception e) {
        throw new RuntimeException(
            " expecting '" + s + "' to have format 'key0:value0,key1:value2' " + e
        );
    }
  }

  static class OrderedIterator implements Iterator<String> {

    Iterator<String> it;

    public OrderedIterator(Iterator<String> iter) {
      SortedSet<String> keys = new TreeSet<String>();
      while (iter.hasNext()) {
        keys.add(iter.next());
      }
      it = keys.iterator();
    }

    public boolean hasNext() {
      return it.hasNext();
    }

    public String next() {
      return it.next();
    }

    public void remove() {
      it.remove();
    }
  }

}
