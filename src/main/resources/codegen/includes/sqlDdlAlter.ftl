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
    <INDEX> id = CompoundIdentifier() <RENAME> <TO> newName = CompoundIdentifier() {
        return SqlDdlAlterNodes.alterIndex(s.end(this), id, newName);
    }
}

SqlDdlAlter SqlAlterSchema(Span s) :
{
    final SqlIdentifier id;
    final SqlIdentifier newName;
}
{
    <SCHEMA> id = CompoundIdentifier() <RENAME> <TO> newName = CompoundIdentifier() {
        return SqlDdlAlterNodes.alterSchema(s.end(this), id, newName);
    }
}
