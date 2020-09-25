<#--
// Copyright 2016-2020 The Polypheny Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
 * Parses a {@code ALTER} DDL statement.
 */
SqlDdlAlter SqlDdlAlter() :
{
    final Span s;
    final SqlDdlAlter alter;
}
{
    <ALTER> { s = span(); }
    (
        alter = SqlAlterIndex(s)
    |
        alter = SqlAlterSchema(s)
    |
        alter = SqlAlterTable(s)
    )
    {
        return alter;
    }
}

SqlDdlAlter SqlAlterIndex(Span s) :
{
    final SqlIdentifier id;
    final SqlIdentifier newName;
}
{
    <INDEX> id = CompoundIdentifier() <RENAME> <TO> newName = SimpleIdentifier()
    {
        return SqlDdlAlterNodes.alterIndex(s.end(this), id, newName);
    }
}

SqlDdlAlter SqlAlterSchema(Span s) :
{
    final SqlIdentifier id;
    final SqlIdentifier newName;
}
{
    <SCHEMA> id = CompoundIdentifier() <RENAME> <TO> newName = SimpleIdentifier()
    {
        return SqlDdlAlterNodes.alterSchema(s.end(this), id, newName);
    }
}

SqlDdlAlter SqlAlterTable(Span s) :
{
    final SqlIdentifier id;
    SqlIdentifier constraintName = null;
    final SqlIdentifier newName;
    final SqlIdentifier refName;
    final SqlIdentifier columnName;
    final SqlNode columnDefinition;
    final SqlNode condition;
    final SqlNode defaultValue;
    final SqlNodeList columnList;
    final SqlNodeList refColumnList;
    String onDelete = null;
    String onUpdate = null;
    final SqlDdlAlter alterTable;
}
{
    <TABLE> id = CompoundIdentifier()
    (
        <ADD>
        (
            [ <COLUMN> ] columnDefinition = ColumnDefinition()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.addColumn(s.end(this), id, columnDefinition);
            }
        |
            [ <CONSTRAINT> constraintName = SimpleIdentifier() ]
            (
                <CHECK> condition = ParenthesizedExpression(ExprContext.ACCEPT_NON_QUERY)
                {
                    alterTable = SqlDdlAlterNodes.AlterTable.addConstraintCheck(s.end(this), id, constraintName, condition);
                }
                |
                <FOREIGN> <KEY> columnList = ParenthesizedSimpleIdentifierList() <REFERENCES> refName = CompoundIdentifier() refColumnList = ParenthesizedSimpleIdentifierList()
                [
                    <ON> <DELETE>
                    (
                        <CASCADE> { onDelete = "CASCADE"; }
                    |
                        <SET> <DEFAULT_> { onDelete = "SET DEFAULT"; }
                    |
                        <SET> <NULL> { onDelete = "SET NULL"; }
                    )
                ]
                [
                    <ON> <UPDATE>
                    (
                        <CASCADE> { onUpdate = "CASCADE"; }
                    |
                        <SET> <DEFAULT_> { onUpdate = "SET DEFAULT"; }
                    |
                        <SET> <NULL> { onUpdate = "SET NULL"; }
                    )
                ]
                {
                    alterTable = SqlDdlAlterNodes.AlterTable.addConstraintForeignKey(s.end(this), id, constraintName, columnList, refName, refColumnList, onDelete, onUpdate);
                }
            |
                <PRIMARY> <KEY> columnList = ParenthesizedSimpleIdentifierList()
                {
                    alterTable = SqlDdlAlterNodes.AlterTable.addConstraintPrimaryKey(s.end(this), id, constraintName, columnList);
                }
            |
                <UNIQUE> columnList = ParenthesizedSimpleIdentifierList()
                 {
                     alterTable = SqlDdlAlterNodes.AlterTable.addConstraintUnique(s.end(this), id, constraintName, columnList);
                 }
            )
        |
            <INDEX> constraintName = SimpleIdentifier() <ON> columnList = ParenthesizedSimpleIdentifierList()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.addIndex(s.end(this), id, constraintName, columnList);
            }
        )
    |
        <ALTER> <COLUMN>
        (
            columnName = SimpleIdentifier()
            (
                <RENAME> <TO> newName = SimpleIdentifier()
                {
                    alterTable = SqlDdlAlterNodes.AlterTable.alterColumnRename(s.end(this), id, columnName, newName);
                }
            |
                <SET>
                (
                    <DEFAULT_> defaultValue = Expression(ExprContext.ACCEPT_NON_QUERY)
                    {
                        alterTable = SqlDdlAlterNodes.AlterTable.alterColumnSetDefault(s.end(this), id, columnName, defaultValue);
                    }
                |
                    <NOT> <NULL>
                    {
                        alterTable = SqlDdlAlterNodes.AlterTable.alterColumnSetNullable(s.end(this), id, columnName, ColumnStrategy.NOT_NULLABLE);
                    }
                |
                    <NULL>
                    {
                        alterTable = SqlDdlAlterNodes.AlterTable.alterColumnSetNullable(s.end(this), id, columnName, ColumnStrategy.NULLABLE);
                    }
                )
            )
        |
            columnDefinition = ColumnDefinition()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.alterColumnDefinition(s.end(this), id, columnDefinition);
            }
        )
    |
        <DROP>
        (
            [ <COLUMN> ] columnName = SimpleIdentifier()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.dropColumn(s.end(this), id, columnName);
            }
        |
            <CONSTRAINT> constraintName = SimpleIdentifier()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.dropConstraint(s.end(this), id, constraintName);
            }
        )
    |
        <RENAME> <TO> newName = SimpleIdentifier()
        {
            alterTable = SqlDdlAlterNodes.AlterTable.rename(s.end(this), id, newName);
        }
    )
    {
        return alterTable;
    }
}

SqlNode ColumnDefinition() :
{
    final Span s = Span.of();
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    SqlNode defaultValue = null;
//    boolean identity = false;
//    SqlNode identityStart = null;
//    SqlNode identityIncrement = null;
//    boolean primaryKey = false;
    ColumnStrategy strategy = ColumnStrategy.NULLABLE;
}
{
    id = SimpleIdentifier() type = DataType()
    [
        <DEFAULT_> defaultValue = Expression(ExprContext.ACCEPT_NON_QUERY)
        {
            strategy = ColumnStrategy.DEFAULT;
        }
/*    | -- Currently not supported in Calcite's CREATE TABLE
        <GENERATED> <BY> <DEFAULT_> <AS> <IDENTITY> <LPAREN> <START> <WITH> identityStart = Literal()
        (
            <COMMA> <INCREMENTED> <BY> identityIncrement = Literal()
        |
            { identityIncrement = null; }
        )
        <RPAREN>
        {
            strategy = ColumnStrategy.DEFAULT;
        }*/
    |
        <NOT> <NULL>
        {
            defaultValue = null;
            strategy = ColumnStrategy.NOT_NULLABLE;
        }
    |
        <NULL>
        {
            defaultValue = null;
            strategy = ColumnStrategy.NULLABLE;
        }
    ]
/*    [ -- Currently not supported in Calcite's CREATE TABLE
        <IDENTITY>
        {
            identity = true;
        }
    ]*/
/*    [ -- Currently not supported in Calcite's CREATE TABLE
        <PRIMARY> <KEY>
        {
            primaryKey = true;
        }
    ]*/
    {
        return SqlDdlNodes.column(s.add(id).end(this), id, type.withNullable(strategy == ColumnStrategy.DEFAULT ? null : strategy == ColumnStrategy.NULLABLE), defaultValue, strategy);
    }
}
