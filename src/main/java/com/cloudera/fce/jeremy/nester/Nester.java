package com.cloudera.fce.jeremy.nester;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class Nester {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlc = new HiveContext(jsc);
        
        DataFrame into = sqlc.read().table(args[0]);
        DataFrame from = sqlc.read().table(args[1]);
        String nestedFieldName = args[2];
        String[] keyFieldNames = args[3].split(",");
        String nestedTableName = args[4];
        
        DataFrame nested = nest(into, from, nestedFieldName, keyFieldNames);
        
        nested.write().mode(SaveMode.Overwrite).saveAsTable(nestedTableName);
    }
    
    public static DataFrame nest(DataFrame into, DataFrame from, String nestedFieldName, String[] keyFieldNames) {
        ExtractFieldsFunction extractFieldsFunction = new ExtractFieldsFunction(keyFieldNames);
        JavaPairRDD<List<Object>, Row> keyedIntoRDD = into.javaRDD().keyBy(extractFieldsFunction);
        JavaPairRDD<List<Object>, Row> keyedFromRDD = from.javaRDD().keyBy(extractFieldsFunction);

        NestFunction nestFunction = new NestFunction();
        JavaRDD<Row> nestedRDD = keyedIntoRDD.cogroup(keyedFromRDD).values().map(nestFunction);
        
        StructType nestedSchema = into.schema().add(nestedFieldName, DataTypes.createArrayType(from.schema()));
        
        DataFrame nested = into.sqlContext().createDataFrame(nestedRDD, nestedSchema);
        
        return nested;
    }
    
    @SuppressWarnings("serial")
    private static class ExtractFieldsFunction implements Function<Row, List<Object>> {
        private String[] fieldNames;
        
        public ExtractFieldsFunction(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }
        
        @Override
        public List<Object> call(Row row) throws Exception {
            List<Object> values = new ArrayList<>();
            
            for (String fieldName : fieldNames) {
                values.add(row.get(row.fieldIndex(fieldName)));
            }
            
            return values;
        }
    }
    
    @SuppressWarnings("serial")
    private static class NestFunction implements Function<Tuple2<Iterable<Row>, Iterable<Row>>, Row> {
        @Override
        public Row call(Tuple2<Iterable<Row>, Iterable<Row>> cogrouped) throws Exception {
            // There should only be one 'into' record per key
            Row intoRow = cogrouped._1().iterator().next();
            Iterable<Row> fromRows = cogrouped._2();
            int intoRowNumFields = intoRow.size();
            
            Object[] nestedValues = new Object[intoRowNumFields + 1];
            for (int i = 0; i < intoRowNumFields; i++) {
                nestedValues[i] = intoRow.get(i);
            }
            nestedValues[intoRowNumFields] = fromRows;
            
            Row nested = RowFactory.create(nestedValues);
            
            return nested;
        }
    }

}
