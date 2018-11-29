package org.nexus.hive.udf;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@UDFType(deterministic = false)
@Description(name = "anynull", value = "Returns column names which have NULL value for given row.", extended = "Takes" +
        " any random string or column as input and scans through all the columns for the row to identify any NULL " +
        "values.")
public class GenericAnyColumnNullUDF extends GenericUDF {
    /**
     * A placeholder for result.
     */
    private final Text result = new Text();

    /**
     * Array to store column names for strings
     */
    private String[] columnNames = null;

    /**
     * Converters for retrieving the arguments to the UDF.
     */
    private transient ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //We are expecting variable number of arguments as parameter to the UDF, so no check for argument length
        //        if (arguments.length != 1) {
        //            throw new UDFArgumentLengthException("_FUNC_ expects exactly 1 argument");
        //        }
        String databaseName = SessionState.get().getCurrentDatabase();

        String lastCommand = SessionState.get().getLastCommand().toLowerCase();

        //System.out.println("Last Command > " + lastCommand);

        Pattern pattern = Pattern.compile(" from (.*?) ");
        Matcher matcher = pattern.matcher(lastCommand);
        String tableName = null;

        int j = 0;
        while (matcher.find()) {
            j++;
            //If there is DB name prefixed to the tableName, then we split and get the table name
            tableName = matcher.group(1);
            if (tableName.contains(".")) {
                tableName = matcher.group(1).split(".")[1];
            }
        }

        //System.out.println("TableName > " + tableName);

        if (j != 1) {
            //We have more than 1 from statement in the query, then this UDF will not be able to support that in the
            // current form
            //Same holds true for no matches
            return null;
        }

        try {
            HiveMetaStoreClient client = new HiveMetaStoreClient(SessionState.getSessionConf());
            List<FieldSchema> fields = client.getFields(databaseName, tableName);

            columnNames = new String[fields.size()];

            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = fields.get(i).getName();
            }

        } catch (Exception e) {
            System.out.println("Exception thrown" + e.getMessage());
        }

//        for (int i = 0; i < arguments.length; i++) {
//            System.out.println("argument [" + i + "] type is [" + arguments[i]
//                    .getTypeName() + "] , category is [" + arguments[i]
//                    .getCategory() + "], and toString is [" + arguments[i].toString());
//        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i].getCategory() == ObjectInspector.Category.PRIMITIVE) {
                converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            } else {
                throw new UDFArgumentTypeException(i,
                        "A primitive argument type was expected but an argument of category " + arguments[i]
                                .getCategory()
                                + " was given.");
            }
        }

//        if (currentUser == null) {
//            String sessUserFromAuth = SessionState.getUserFromAuthenticator();
//            if (sessUserFromAuth != null) {
//                currentUser = new Text(sessUserFromAuth);
//            }
//        }


        // We will be returning a Text object
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        String indexes = "";
        String colNames = "";

        for (int i = 0; i < converters.length; i++) {
            Object value = converters[i].convert(arguments[i].get());

            if (value == null) {
                indexes = indexes + i + ",";
                colNames = colNames + columnNames[i] + ",";

                //System.out.println("index [" + i + "] , colume [" + columnNames[i] + "]");
            }
        }

        result.set(indexes + ";" + colNames);

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("anynull", children);
    }
}
