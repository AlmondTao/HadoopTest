package udf;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author TaoQingYang
 * @date 2022/11/11
 */
public class MyUdf extends GenericUDF {


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentException("Input Args Length Error!!!");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }


        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0] == null){
            return 0;
        }

        return deferredObjects[0].get().toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "嗨嗨害";
    }
}
