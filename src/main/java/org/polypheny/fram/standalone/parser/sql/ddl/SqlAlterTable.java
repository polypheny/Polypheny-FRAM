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

package org.polypheny.fram.standalone.parser.sql.ddl;


import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.polypheny.fram.standalone.parser.sql.SqlDdlAlter;


/**
 * Parse tree for {@code ALTER SCHEMA <schemaname> RENAME TO <newname>;} statement.
 */
public abstract class SqlAlterTable extends SqlDdlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER TABLE", SqlKind.ALTER_TABLE );

    protected final SqlIdentifier tableName;


    protected SqlAlterTable( SqlParserPos pos, SqlIdentifier tableName ) {
        super( OPERATOR, pos );
        this.tableName = Objects.requireNonNull( tableName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        tableName.unparse( writer, leftPrec, rightPrec );
    }


    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( tableName );
    }


    protected static abstract class SqlAlterTableAddConstraint extends SqlAlterTable {

        protected final SqlIdentifier constraintName;


        protected SqlAlterTableAddConstraint( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName ) {
            super( pos, tableName );

            this.constraintName = constraintName; // nullable
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( constraintName )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );

            writer.keyword( "ADD" );
            if ( constraintName != null ) {
                writer.keyword( "CONSTRAINT" );
                constraintName.unparse( writer, leftPrec, rightPrec );
            }
        }
    }


    /**
     * {@code ALTER TABLE <tablename> ADD [CONSTRAINT <constraintname>]
     * CHECK (<search condition>);}
     */
    public static class SqlAlterTableAddCheck extends SqlAlterTableAddConstraint {

        private final SqlNode condition;


        public SqlAlterTableAddCheck( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName, SqlNode condition ) {
            super( pos, tableName, constraintName );

            this.condition = Objects.requireNonNull( condition );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( condition )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );

            writer.keyword( "CHECK" );
            condition.unparse( writer, leftPrec, rightPrec ); // prints already parenthesis around the expression
        }
    }


    /**
     * {@code ALTER TABLE <tablename>
     * ADD [CONSTRAINT <constraintname>] FOREIGN KEY (<column list>)
     * REFERENCES <exptablename> (<column list>)
     * [ON {DELETE | UPDATE} {CASCADE | SET DEFAULT | SET NULL}];}
     */
    public static class SqlAlterTableAddForeignKey extends SqlAlterTableAddConstraint {

        private final SqlNodeList columnList;
        private final SqlIdentifier refName;
        private final SqlNodeList refColumnList;
        private final SqlAlterTableForeignKeyOption onDelete;
        private final SqlAlterTableForeignKeyOption onUpdate;


        public SqlAlterTableAddForeignKey( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName, SqlNodeList columnList, SqlIdentifier refName, SqlNodeList refColumnList, String onDelete, String onUpdate ) {
            super( pos, tableName, constraintName );

            this.columnList = Objects.requireNonNull( columnList );
            this.refName = Objects.requireNonNull( refName );
            this.refColumnList = Objects.requireNonNull( refColumnList );
            this.onDelete = onDelete == null ? null : SqlAlterTableForeignKeyOption.valueOf( onDelete.toUpperCase().replace( ' ', '_' ) );
            this.onUpdate = onUpdate == null ? null : SqlAlterTableForeignKeyOption.valueOf( onUpdate.toUpperCase().replace( ' ', '_' ) );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( columnList, refName, refColumnList )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );

            writer.keyword( "FOREIGN" );
            writer.keyword( "KEY" );
            {//NOSONAR "squid:S1199" - Justification: better readability
                final SqlWriter.Frame list = writer.startList( FrameTypeEnum.PARENTHESES, "(", ")" );
                columnList.unparse( writer, leftPrec, rightPrec );
                writer.endList( list );
            }
            writer.keyword( "REFERENCES" );
            refName.unparse( writer, leftPrec, rightPrec );
            {//NOSONAR "squid:S1199" - Justification: better readability
                final SqlWriter.Frame list = writer.startList( FrameTypeEnum.PARENTHESES, "(", ")" );
                refColumnList.unparse( writer, leftPrec, rightPrec );
                writer.endList( list );
            }
            if ( onDelete != null ) {
                writer.keyword( "ON" );
                writer.keyword( "DELETE" );
                writer.keyword( onDelete.toString() );
            }
            if ( onUpdate != null ) {
                writer.keyword( "ON" );
                writer.keyword( "UPDATE" );
                writer.keyword( onUpdate.toString() );
            }
        }


        private enum SqlAlterTableForeignKeyOption {
            CASCADE,
            SET_DEFAULT,
            SET_NULL,
            ;


            @Override
            public String toString() {
                return super.toString().replace( '_', ' ' );
            }
        }
    }


    /**
     * {@code ALTER TABLE <tablename> ADD [CONSTRAINT <constraintname>]
     * PRIMARY KEY (<column list>);}
     */
    public static class SqlAlterTableAddPrimaryKey extends SqlAlterTableAddConstraint {

        private final SqlNodeList columnList;


        public SqlAlterTableAddPrimaryKey( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName, SqlNodeList columnList ) {
            super( pos, tableName, constraintName );

            this.columnList = Objects.requireNonNull( columnList );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( columnList )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );

            writer.keyword( "PRIMARY" );
            writer.keyword( "KEY" );
            {//NOSONAR "squid:S1199" - Justification: better readability
                final SqlWriter.Frame list = writer.startList( FrameTypeEnum.PARENTHESES, "(", ")" );
                columnList.unparse( writer, leftPrec, rightPrec );
                writer.endList( list );
            }
        }
    }


    /**
     * {@code ALTER TABLE <tablename> ADD [CONSTRAINT <constraintname>] UNIQUE (<column list>);}
     */
    public static class SqlAlterTableAddUnique extends SqlAlterTableAddConstraint {

        private final SqlNodeList columnList;


        public SqlAlterTableAddUnique( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName, SqlNodeList columnList ) {
            super( pos, tableName, constraintName );

            this.columnList = Objects.requireNonNull( columnList );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( columnList )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );

            writer.keyword( "UNIQUE" );
            {//NOSONAR "squid:S1199" - Justification: better readability
                final SqlWriter.Frame list = writer.startList( FrameTypeEnum.PARENTHESES, "(", ")" );
                columnList.unparse( writer, leftPrec, rightPrec );
                writer.endList( list );
            }
        }
    }


    /**
     * {@code ALTER TABLE <tablename> DROP CONSTRAINT <constraintname>;}
     */
    public static class SqlAlterTableDropConstraint extends SqlAlterTable {

        private final SqlIdentifier constraintName;


        public SqlAlterTableDropConstraint( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier constraintName ) {
            super( pos, tableName );
            this.constraintName = Objects.requireNonNull( constraintName );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( constraintName )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );
            writer.keyword( "DROP" );
            writer.keyword( "CONSTRAINT" );
            constraintName.unparse( writer, leftPrec, rightPrec );
        }
    }


    /**
     * {@code ALTER TABLE <tablename> RENAME TO <newname>;}
     */
    public static class SqlAlterTableRename extends SqlAlterTable {

        private final SqlIdentifier newName;


        public SqlAlterTableRename( SqlParserPos pos, SqlIdentifier tableName, SqlIdentifier newName ) {
            super( pos, tableName );
            this.newName = Objects.requireNonNull( newName );
        }


        @Nonnull
        @Override
        public List<SqlNode> getOperandList() {
            return ImmutableNullableList.<SqlNode>builder().addAll( super.getOperandList() )
                    .add( newName )
                    .build();
        }


        @Override
        public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
            super.unparse( writer, leftPrec, rightPrec );
            writer.keyword( "RENAME" );
            writer.keyword( "TO" );
            newName.unparse( writer, leftPrec, rightPrec );
        }
    }
}
