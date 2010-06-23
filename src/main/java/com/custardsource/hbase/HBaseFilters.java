package com.custardsource.hbase;


public class HBaseFilters {
    

// public static Filter columnValueMatches(final byte[] columnName, final String regularExpression)
    // {

//        TODO  according to HBase docs, custom filters need to be replicated in ALL cluster nodes, because the code runs locally... hrm!!
        
//        return new Filter() {
//            String regExp = regularExpression;
//            Pattern pattern = Pattern.compile(regExp);
//            byte[] column = columnName;
//
//            boolean foundMatch = false;
//
//            @Override
//            public boolean filterAllRemaining() {
//                return false;
//            }
//
//            @Override
//            public ReturnCode filterKeyValue(KeyValue v) {
//                if (!v.getColumn().equals(column)) {
//                    return ReturnCode.INCLUDE; // we don't care about this column
//                }
//                String cellValue = Bytes.toString(v.getValue());
//                if (pattern.matcher(cellValue).matches()) {
//                    return ReturnCode.INCLUDE;
//                } else {
//                    return ReturnCode.NEXT_ROW;
//                }
//            }
//
//            @Override
//            public boolean filterRow() {
//                return !foundMatch;
//            }
//
//            @Override
//            public boolean filterRowKey(byte[] buffer, int offset, int length) {
//                return false;
//            }
//
//            @Override
//            public void reset() {
//                foundMatch = false;
//            }
//
//            @Override
//            public void readFields(DataInput in) throws IOException {
//                column = Bytes.readByteArray(in);
//                regExp = in.readUTF();
//                pattern = Pattern.compile(regExp);
//                foundMatch = in.readBoolean();
//            }
//
//            @Override
//            public void write(DataOutput out) throws IOException {
//                Bytes.writeByteArray(out, column);
//                out.writeUTF(regExp);
//                out.writeBoolean(foundMatch);
//            }
//        };
//        
//    }

}
