/*
 * Copyright 2016-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.fram.standalone.parser.sql;


import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;


public abstract class SqlNodeUtils {

    public static final SqlIdentifierUtils IDENTIFIER_UTILS = new SqlIdentifierUtils();
    public static final SqlSelectUtils SELECT_UTILS = new SqlSelectUtils();
    public static final SqlInsertUtils INSERT_UTILS = new SqlInsertUtils();
    public static final SqlDeleteUtils DELETE_UTILS = new SqlDeleteUtils();
    public static final SqlUpdateUtils UPDATE_UTILS = new SqlUpdateUtils();


    private SqlNodeUtils() {
    }


    public static int getNumberOfDynamicParameters( final SqlNode sql ) {
        return sql.accept( new SqlVisitor<Integer>() {
            @Override
            public Integer visit( SqlLiteral literal ) {
                return 0;
            }


            @Override
            public Integer visit( SqlCall call ) {
                int numberOfDynamicParameters = 0;
                for ( SqlNode node : call.getOperandList() ) {
                    if ( node != null ) {
                        numberOfDynamicParameters += node.accept( this );
                    }
                }
                return numberOfDynamicParameters;
            }


            @Override
            public Integer visit( SqlNodeList nodeList ) {
                int numberOfDynamicParameters = 0;
                for ( SqlNode node : nodeList ) {
                    if ( node != null ) {
                        numberOfDynamicParameters += node.accept( this );
                    }
                }
                return numberOfDynamicParameters;
            }


            @Override
            public Integer visit( SqlIdentifier id ) {
                return 0;
            }


            @Override
            public Integer visit( SqlDataTypeSpec type ) {
                return 0;
            }


            @Override
            public Integer visit( SqlDynamicParam param ) {
                return 1;
            }


            @Override
            public Integer visit( SqlIntervalQualifier intervalQualifier ) {
                throw new UnsupportedOperationException( "Not implemented yet." );
            }
        } );
    }


    public static Map<String, Integer> getColumnsNameToDynamicParametersIndexMap( final SqlNode sql ) {
        return sql.accept( new SqlVisitor<Map<String, Integer>>() {
            @Override
            public Map<String, Integer> visit( SqlLiteral literal ) {
                return Collections.emptyMap();
            }


            @Override
            public Map<String, Integer> visit( SqlCall call ) {
                Map<String, Integer> m = new HashMap<>();
                if ( call.isA( SqlKind.BINARY_COMPARISON ) ) {
                    if ( call.operand( 0 ).isA( EnumSet.of( SqlKind.IDENTIFIER ) ) && call.operand( 1 ).isA( EnumSet.of( SqlKind.DYNAMIC_PARAM ) ) ) {
                        final SqlIdentifier column = call.<SqlIdentifier>operand( 0 );
                        final int dynamicParameterIndex = call.<SqlDynamicParam>operand( 1 ).getIndex();
                        if ( column.isSimple() ) {
                            m.put( column.getSimple(), dynamicParameterIndex );
                        } else {
                            m.put( column.names.reverse().get( 0 ), dynamicParameterIndex );
                        }
                    } else if ( call.operand( 0 ).isA( EnumSet.of( SqlKind.DYNAMIC_PARAM ) ) && call.operand( 1 ).isA( EnumSet.of( SqlKind.IDENTIFIER ) ) ) {
                        final SqlIdentifier column = call.<SqlIdentifier>operand( 1 );
                        final int dynamicParameterIndex = call.<SqlDynamicParam>operand( 0 ).getIndex();
                        if ( column.isSimple() ) {
                            m.put( column.getSimple(), dynamicParameterIndex );
                        } else {
                            m.put( column.names.reverse().get( 0 ), dynamicParameterIndex );
                        }
                    } else {
                        for ( SqlNode n : call.getOperandList() ) {
                            if ( n != null ) {
                                m.putAll( n.accept( this ) );
                            }
                        }
                    }
                } else {
                    for ( SqlNode n : call.getOperandList() ) {
                        if ( n != null ) {
                            m.putAll( n.accept( this ) );
                        }
                    }
                }
                return m;
            }


            @Override
            public Map<String, Integer> visit( SqlNodeList nodeList ) {
                Map<String, Integer> m = new HashMap<>();
                for ( SqlNode n : nodeList ) {
                    m.putAll( n.accept( this ) );
                }
                return m;
            }


            @Override
            public Map<String, Integer> visit( SqlIdentifier id ) {
                return Collections.emptyMap();
            }


            @Override
            public Map<String, Integer> visit( SqlDataTypeSpec type ) {
                return Collections.emptyMap();
            }


            @Override
            public Map<String, Integer> visit( SqlDynamicParam param ) {
                return Collections.emptyMap();
            }


            @Override
            public Map<String, Integer> visit( SqlIntervalQualifier intervalQualifier ) {
                return Collections.emptyMap();
            }
        } );
    }


    protected boolean whereConditionContainsOnlyEquals( final SqlNode condition ) {
        return !condition.accept( new SqlVisitor<Boolean>() {
            // Do we contain anything else than EQUALS?
            @Override
            public Boolean visit( SqlCall call ) {
                if ( call.isA( EnumSet.of( SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.IS_DISTINCT_FROM, SqlKind.IS_NOT_DISTINCT_FROM ) ) ) {
                    return Boolean.TRUE;
                }

                for ( SqlNode n : call.getOperandList() ) {
                    if ( n != null && n.accept( this ) ) {
                        return Boolean.TRUE;
                    }
                }

                return Boolean.FALSE;
            }


            @Override
            public Boolean visit( SqlNodeList nodeList ) {
                for ( SqlNode n : nodeList ) {
                    if ( n != null && n.accept( this ) ) {
                        return Boolean.TRUE;
                    }
                }

                return Boolean.FALSE;
            }


            @Override
            public Boolean visit( SqlIdentifier id ) {
                return Boolean.FALSE;
            }


            @Override
            public Boolean visit( SqlLiteral literal ) {
                return Boolean.FALSE;
            }


            @Override
            public Boolean visit( SqlIntervalQualifier intervalQualifier ) {
                throw new UnsupportedOperationException( "Not implemented yet." );
            }


            @Override
            public Boolean visit( SqlDataTypeSpec type ) {
                return Boolean.FALSE;
            }


            @Override
            public Boolean visit( SqlDynamicParam param ) {
                return Boolean.FALSE;
            }
        } );
    }


    public static class SqlIdentifierUtils extends SqlNodeUtils {

        private SqlIdentifierUtils() {
        }


        /**
         * Get the part of the compound identifier using the following indexes: ... . 3 (Catalog) . 2 (Schema) . 1 (Table) . 0 (Column)
         * 0: The very last part of the name. Can be a Column, Table, or Schema name --- so it is context dependent! Is is always present.
         * 1: The part before 0.
         *
         * @return The part of the compound identifier as String or null if not present.
         */
        public String getPartOfCompoundIdentifier( final SqlIdentifier compoundIdentifier, final int partIndex ) {
            if ( partIndex < compoundIdentifier.names.size() ) {
                return compoundIdentifier.names.reverse().get( partIndex );
            } else {
                return null;
            }
        }
    }


    public static class SqlSelectUtils extends SqlNodeUtils {

        private SqlSelectUtils() {
        }


        public boolean selectListContainsAggregate( final SqlSelect select ) {
            return select.getSelectList().accept( new SqlVisitor<Boolean>() {
                @Override
                public Boolean visit( SqlNodeList nodeList ) {
                    boolean isAggregate = false;
                    for ( SqlNode n : nodeList.getList() ) {
                        isAggregate |= n.accept( this );
                    }
                    return isAggregate;
                }


                @Override
                public Boolean visit( SqlLiteral literal ) {
                    return Boolean.FALSE;
                }


                @Override
                public Boolean visit( SqlCall call ) {
                    if ( call.isA( SqlKind.AGGREGATE ) ) {
                        return Boolean.TRUE;
                    }

                    boolean isAggregate = false;
                    for ( SqlNode n : call.getOperandList() ) {
                        isAggregate |= n.accept( this );
                    }
                    return isAggregate;
                }


                @Override
                public Boolean visit( SqlIdentifier id ) {
                    return Boolean.FALSE;
                }


                @Override
                public Boolean visit( SqlDataTypeSpec type ) {
                    return Boolean.FALSE;
                }


                @Override
                public Boolean visit( SqlDynamicParam param ) {
                    return Boolean.FALSE;
                }


                @Override
                public Boolean visit( SqlIntervalQualifier intervalQualifier ) {
                    return Boolean.FALSE;
                }
            } );
        }


        public SqlIdentifier getTargetTable( final SqlSelect select ) {
            return select.getFrom().accept( new SqlVisitor<SqlIdentifier>() {
                @Override
                public SqlIdentifier visit( SqlIdentifier id ) {
                    return id;
                }


                @Override
                public SqlIdentifier visit( SqlDataTypeSpec type ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlDynamicParam param ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlIntervalQualifier intervalQualifier ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlLiteral literal ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlCall call ) {
                    if ( call.getKind() == SqlKind.AS ) {
                        return call.operand( 0 );
                    }

                    return call.getOperator().acceptCall( this, call );
                }


                @Override
                public SqlIdentifier visit( SqlNodeList nodeList ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
            } );
        }


        public boolean whereConditionContainsOnlyEquals( final SqlSelect select ) {
            return super.whereConditionContainsOnlyEquals( select.getWhere() );
        }


        public SortedMap<Integer, Integer> getPrimaryKeyColumnsIndexesToParametersIndexesMap( final SqlSelect select, final Map<String, Integer> primaryKeyColumnNamesAndIndexes ) {
            final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();

            final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( select );
            // Check if the primary key is included in the WHERE condition

            for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
                // for every primary key and its index in the table
                final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
                if ( parameterIndex != null ) {
                    // the primary key is present in the condition
                    primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
                } else {
                    // the primary key is NOT in the condition
                    // - later, we need to compare if this result contains all primary key columns
                    // - if this is not the case, we need to scan the result set to gain the analytics instead of estimating it by using the input values
                    // - this might not be as precise but for the two benchmarks (tpcc and ycsb) this should be enought
                    //throw new UnsupportedOperationException( "Not implemented yet." );
                }
            }

            return primaryKeyColumnsIndexesToParametersIndexes;
        }
    }


    public static class SqlInsertUtils extends SqlNodeUtils {

        private SqlInsertUtils() {
        }


        public SqlIdentifier getTargetTable( final SqlInsert insert ) {
            return insert.getTargetTable().accept( new SqlVisitor<SqlIdentifier>() {
                @Override
                public SqlIdentifier visit( SqlLiteral literal ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlCall call ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlNodeList nodeList ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlIdentifier id ) {
                    return id;
                }


                @Override
                public SqlIdentifier visit( SqlDataTypeSpec type ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlDynamicParam param ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlIntervalQualifier intervalQualifier ) {
                    throw new IllegalStateException();
                }
            } );
        }


        public SqlNodeList getTargetColumns( final SqlInsert insert ) {
            return insert.getTargetColumnList();
        }


        public SortedMap<Integer, Integer> getPrimaryKeyColumnsIndexesToParametersIndexesMap( final SqlInsert insert, final Map<String, Integer> primaryKeyColumnNamesAndIndexes ) {
            final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();
            final SqlNodeList targetColumns = insert.getTargetColumnList();

            if ( targetColumns == null || targetColumns.size() == 0 ) {
                // The insert statement has to provide values for every column in the order of the columns
                // Thus, we can use the indexes of the primary key columns to find their corresponding values
                for ( int primaryKeyColumnIndex : new TreeSet<>( primaryKeyColumnNamesAndIndexes.values() ) /* naturally ordered and thus the indexes of the primary key columns are in the correct order */ ) {
                    // The primary key FOO with the column index 2
                    // maps to the 3rd parameter value
                    primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnIndex, primaryKeyColumnIndex );
                }
            } else {
                // search in targetColumns for the index of the primary keys in this PreparedStatement
                for ( int columnIndex = 0; columnIndex < targetColumns.size(); ++columnIndex ) {
                    SqlNode node = targetColumns.get( columnIndex );
                    if ( node.isA( EnumSet.of( SqlKind.IDENTIFIER ) ) ) {
                        final SqlIdentifier targetColumn = (SqlIdentifier) node;
                        final Integer primaryKeyIndex = primaryKeyColumnNamesAndIndexes.get( SqlNodeUtils.IDENTIFIER_UTILS.getPartOfCompoundIdentifier( targetColumn, 0 ) );
                        if ( primaryKeyIndex != null ) {
                            // The name was in the map
                            primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyIndex, columnIndex );
                        }
                    } else {
                        // node IS NOT a SqlKind.IDENTIFIER
                        throw new UnsupportedOperationException( "Not implemented yet." );
                    }
                }
            }
            return primaryKeyColumnsIndexesToParametersIndexes;
        }
    }


    public static class SqlDeleteUtils extends SqlNodeUtils {

        private SqlDeleteUtils() {
        }


        public SqlIdentifier getTargetTable( final SqlDelete delete ) {
            return delete.getTargetTable().accept( new SqlVisitor<SqlIdentifier>() {
                @Override
                public SqlIdentifier visit( SqlLiteral literal ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlCall call ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlNodeList nodeList ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlIdentifier id ) {
                    return id;
                }


                @Override
                public SqlIdentifier visit( SqlDataTypeSpec type ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlDynamicParam param ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlIntervalQualifier intervalQualifier ) {
                    throw new IllegalStateException();
                }
            } );
        }


        public boolean whereConditionContainsOnlyEquals( final SqlDelete delete ) {
            return super.whereConditionContainsOnlyEquals( delete.getCondition() );
        }


        public Map<Integer, Integer> getPrimaryKeyColumnsIndexesToParametersIndexesMap( final SqlDelete delete, final Map<String, Integer> primaryKeyColumnNamesAndIndexes ) {
            final Map<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new HashMap<>();

            final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( delete );
            // Check if the primary key is included in the WHERE condition

            for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
                // for every primary key and its index in the table
                final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
                if ( parameterIndex != null ) {
                    // the primary key is present in the condition
                    primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
                } else {
                    // the primary key is NOT in the condition
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
            }

            return primaryKeyColumnsIndexesToParametersIndexes;
        }
    }


    public static class SqlUpdateUtils extends SqlNodeUtils {

        private SqlUpdateUtils() {
        }


        public SqlIdentifier getTargetTable( final SqlUpdate update ) {
            return update.getTargetTable().accept( new SqlVisitor<SqlIdentifier>() {
                @Override
                public SqlIdentifier visit( SqlLiteral literal ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlCall call ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlNodeList nodeList ) {
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }


                @Override
                public SqlIdentifier visit( SqlIdentifier id ) {
                    return id;
                }


                @Override
                public SqlIdentifier visit( SqlDataTypeSpec type ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlDynamicParam param ) {
                    throw new IllegalStateException();
                }


                @Override
                public SqlIdentifier visit( SqlIntervalQualifier intervalQualifier ) {
                    throw new IllegalStateException();
                }
            } );
        }


        public SqlNodeList getTargetColumns( final SqlUpdate update ) {
            return update.getTargetColumnList();
        }

        public boolean targetColumnsContainPrimaryKeyColumn(final SqlUpdate update, final Set<String> primaryKeyColumnsNames ) {
            return this.getTargetColumns( update ).accept( new SqlBasicVisitor<Boolean>() {
                @Override
                public Boolean visit( SqlNodeList nodeList ) {
                    boolean b = false;
                    for ( SqlNode node : nodeList ) {
                        b |= node.accept( this );
                    }
                    return b;
                }


                @Override
                public Boolean visit( SqlIdentifier id ) {
                    return primaryKeyColumnsNames.contains( id.names.reverse().get( 0 ) );
                }
            } );
        }

        public boolean whereConditionContainsOnlyEquals( final SqlUpdate update ) {
            return super.whereConditionContainsOnlyEquals( update.getCondition() );
        }


        public SortedMap<Integer, Integer> getPrimaryKeyColumnsIndexesToParametersIndexesMap( final SqlUpdate update, final Map<String, Integer> primaryKeyColumnNamesAndIndexes ) {
            final SortedMap<Integer, Integer> primaryKeyColumnsIndexesToParametersIndexes = new TreeMap<>();

            final Map<String, Integer> columnNamesToParameterIndexes = SqlNodeUtils.getColumnsNameToDynamicParametersIndexMap( update );
            // Check if the primary key is included in the WHERE condition

            for ( Entry<String, Integer> primaryKeyColumnNameAndIndex : primaryKeyColumnNamesAndIndexes.entrySet() ) {
                // for every primary key and its index in the table
                final Integer parameterIndex = columnNamesToParameterIndexes.get( primaryKeyColumnNameAndIndex.getKey() );
                if ( parameterIndex != null ) {
                    // the primary key is present in the condition
                    primaryKeyColumnsIndexesToParametersIndexes.put( primaryKeyColumnNameAndIndex.getValue(), parameterIndex );
                } else {
                    // the primary key is NOT in the condition
                    // Thus, during execution, the statement has to be executed on all nodes
                    throw new UnsupportedOperationException( "Not implemented yet." );
                }
            }

            return primaryKeyColumnsIndexesToParametersIndexes;
        }
    }
}
