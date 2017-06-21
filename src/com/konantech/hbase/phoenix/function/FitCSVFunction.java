package com.konantech.hbase.phoenix.function;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import sun.applet.AppletViewer;

/**
 * Created by myungjoonlee on 2017. 6. 20..
 */
@BuiltInFunction(
        name= FitCSVFunction.NAME,
        args={
                @Argument(allowedTypes={PVarchar.class})
        }
)
public class FitCSVFunction extends ScalarFunction {
    public static final String NAME = "FIT_CSV";

    public FitCSVFunction() {
        AppletViewer viwer;
    }

    public FitCSVFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * is the method to be implemented which provides access to the Tuple
     *
     * @param tuple
     *      Single row result during scan iteration
     * @param ptr
     *      Pointer to byte value being accessed
     * @return
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int length = ptr.getLength();
        if (length == 0) {
            return true;
        }

        int offset = ptr.getOffset();
        String source = Bytes.toString(ptr.get(), offset, length);
        String modified = source.replaceAll("[\r\n\t]+" ,"");

        ptr.set(modified.getBytes());

        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
