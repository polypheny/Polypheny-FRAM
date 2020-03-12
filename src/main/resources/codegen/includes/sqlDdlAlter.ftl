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
    final SqlIdentifier constraintName;
    final SqlIdentifier newName;
    final SqlIdentifier refName;
    final SqlIdentifier columnName;
    final SqlNode condition;
    final SqlNodeList columnList;
    final SqlNodeList refColumnList;
    final String onDelete;
    final String onUpdate;
    final SqlDdlAlter alterTable;
}
{
    <TABLE> id = CompoundIdentifier()
    (
        <ADD>
        (
            <CONSTRAINT> constraintName = SimpleIdentifier()
        |
            { constraintName = null; }
        )
        (
            <CHECK> condition = ParenthesizedExpression(ExprContext.ACCEPT_NON_QUERY)
            {
                alterTable = SqlDdlAlterNodes.AlterTable.addCheck(s.end(this), id, constraintName, condition);
            }
            |
            <FOREIGN> <KEY> columnList = ParenthesizedSimpleIdentifierList() <REFERENCES> refName = CompoundIdentifier() refColumnList = ParenthesizedSimpleIdentifierList()
            (
                <ON> <DELETE>
                (
                    <CASCADE> { onDelete = "CASCADE"; }
                |
                    <SET> <DEFAULT_> { onDelete = "SET DEFAULT"; }
                |
                    <SET> <NULL> { onDelete = "SET NULL"; }
                )
            |
                { onDelete = null; }
            )
            (
                <ON> <UPDATE>
                (
                    <CASCADE> { onUpdate = "CASCADE"; }
                |
                    <SET> <DEFAULT_> { onUpdate = "SET DEFAULT"; }
                |
                    <SET> <NULL> { onUpdate = "SET NULL"; }
                )
            |
                { onUpdate = null; }
            )
            {
                alterTable = SqlDdlAlterNodes.AlterTable.addForeignKey(s.end(this), id, constraintName, columnList, refName, refColumnList, onDelete, onUpdate);
            }
        |
            <PRIMARY> <KEY> columnList = ParenthesizedSimpleIdentifierList()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.addPrimaryKey(s.end(this), id, constraintName, columnList);
            }
        |
            <UNIQUE> columnList = ParenthesizedSimpleIdentifierList()
             {
                 alterTable = SqlDdlAlterNodes.AlterTable.addUnique(s.end(this), id, constraintName, columnList);
             }
        )
    |
        <ALTER> <COLUMN> columnName = SimpleIdentifier()
        (
            <RENAME> <TO> newName = SimpleIdentifier()
            {
                alterTable = SqlDdlAlterNodes.AlterTable.alterColumnRename(s.end(this), id, columnName, newName);
            }
        )
    |
        <DROP>
        (
            (
                <COLUMN> columnName = SimpleIdentifier()
            |
                columnName = SimpleIdentifier()
            )
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
