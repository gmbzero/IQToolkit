using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal abstract class MappingTable
    {

    }

    internal abstract class AdvancedMapping : BasicMapping
    {
        public abstract bool IsNestedEntity(MappingEntity entity, MemberInfo member);

        public abstract IList<MappingTable> GetTables(MappingEntity entity);

        public abstract string GetAlias(MappingTable table);

        public abstract string GetAlias(MappingEntity entity, MemberInfo member);

        public abstract string GetTableName(MappingTable table);

        public abstract bool IsExtensionTable(MappingTable table);

        public abstract string GetExtensionRelatedAlias(MappingTable table);

        public abstract IEnumerable<string> GetExtensionKeyColumnNames(MappingTable table);

        public abstract IEnumerable<MemberInfo> GetExtensionRelatedMembers(MappingTable table);

        public override bool IsRelationship(MappingEntity entity, MemberInfo member)
        {
            return base.IsRelationship(entity, member) || this.IsNestedEntity(entity, member);
        }

        public override object CloneEntity(MappingEntity entity, object instance)
        {
            var clone = base.CloneEntity(entity, instance);

            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, mi))
                {
                    var nested = this.GetRelatedEntity(entity, mi);
                    var nestedValue = mi.GetValue(instance);

                    if (nestedValue != null)
                    {
                        var nestedClone = this.CloneEntity(nested, mi.GetValue(instance));

                        mi.SetValue(clone, nestedClone);
                    }
                }
            }

            return clone;
        }

        public override bool IsModified(MappingEntity entity, object instance, object original)
        {
            if (base.IsModified(entity, instance, original))
            {
                return true;
            }

            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, mi))
                {
                    var nested = this.GetRelatedEntity(entity, mi);

                    if (this.IsModified(nested, mi.GetValue(instance), mi.GetValue(original)))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new AdvancedMapper(this, translator);
        }
    }

    internal class AdvancedMapper : BasicMapper
    {
        private readonly AdvancedMapping mapping;

        public AdvancedMapper(AdvancedMapping mapping, QueryTranslator translator) : base(mapping, translator)
        {
            this.mapping = mapping;
        }

        public virtual IEnumerable<MappingTable> GetDependencyOrderedTables(MappingEntity entity)
        {
            var lookup = this.mapping.GetTables(entity).ToLookup(t => this.mapping.GetAlias(t));

            return this.mapping.GetTables(entity).Sort(t => this.mapping.IsExtensionTable(t) ? lookup[this.mapping.GetExtensionRelatedAlias(t)] : null);
        }

        internal override DbEntityExpression GetEntityExpression(Expression root, MappingEntity entity)
        {
            var assignments = new List<EntityAssignment>();

            foreach (var mi in this.mapping.GetMappedMembers(entity))
            {
                if (!this.mapping.IsAssociationRelationship(entity, mi))
                {
                    var me = null as Expression;

                    if (this.mapping.IsNestedEntity(entity, mi))
                    {
                        me = this.GetEntityExpression(root, this.mapping.GetRelatedEntity(entity, mi));
                    }
                    else
                    {
                        me = this.GetMemberExpression(root, entity, mi);
                    }

                    if (me != null)
                    {
                        assignments.Add(new EntityAssignment(mi, me));
                    }
                }
            }

            return new DbEntityExpression(entity, this.BuildEntityExpression(entity, assignments));
        }

        public override Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member)
        {
            if (this.mapping.IsNestedEntity(entity, member))
            {
                var subEntity = this.mapping.GetRelatedEntity(entity, member);

                return this.GetEntityExpression(root, subEntity);
            }
            else
            {
                return base.GetMemberExpression(root, entity, member);
            }
        }

        internal override DbProjectionExpression GetQueryExpression(MappingEntity entity)
        {
            var tables = this.mapping.GetTables(entity);

            if (tables.Count <= 1)
            {
                return base.GetQueryExpression(entity);
            }

            var aliases = new Dictionary<string, TableAlias>();
            var rootTable = tables.Single(ta => !this.mapping.IsExtensionTable(ta));
            var tex = new DbTableExpression(new TableAlias(), entity, this.mapping.GetTableName(rootTable));

            aliases.Add(this.mapping.GetAlias(rootTable), tex.Alias);

            var source = tex as Expression;

            foreach (var table in tables.Where(t => this.mapping.IsExtensionTable(t)))
            {
                var joinedTableAlias = new TableAlias();
                var extensionAlias = this.mapping.GetAlias(table);

                aliases.Add(extensionAlias, joinedTableAlias);

                var keyColumns = this.mapping.GetExtensionKeyColumnNames(table).ToList();
                var relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToList();
                var relatedAlias = this.mapping.GetExtensionRelatedAlias(table);

                aliases.TryGetValue(relatedAlias, out TableAlias relatedTableAlias);

                var joinedTex = new DbTableExpression(joinedTableAlias, entity, this.mapping.GetTableName(table));
                var cond = null as Expression;

                for (int i = 0, n = keyColumns.Count; i < n; i++)
                {
                    var memberType = TypeHelper.GetMemberType(relatedMembers[i]);
                    var colType = this.GetColumnType(entity, relatedMembers[i]);
                    var relatedColumn = new DbColumnExpression(memberType, colType, relatedTableAlias, this.mapping.GetColumnName(entity, relatedMembers[i]));
                    var joinedColumn = new DbColumnExpression(memberType, colType, joinedTableAlias, keyColumns[i]);
                    var eq = joinedColumn.Equal(relatedColumn);

                    cond = (cond != null) ? cond.And(eq) : eq;
                }

                source = new DbJoinExpression(JoinType.SingletonLeftOuter, source, joinedTex, cond);
            }

            var columns = new List<DbColumnDeclaration>();

            this.GetColumns(entity, aliases, columns);

            var root = new DbSelectExpression(new TableAlias(), columns, source, null);
            var existingAliases = aliases.Values.ToArray();

            var projector = this.GetEntityExpression(root, entity) as Expression;
            var selectAlias = new TableAlias();
            var pc = DbColumnProjector.ProjectColumns(this.Translator.Linguist.Language, projector, null, selectAlias, root.Alias);
            var proj = new DbProjectionExpression
            (
                new DbSelectExpression(selectAlias, pc.Columns, root, null),
                pc.Projector
            );

            return this.Translator.Police.ApplyPolicy(proj, entity.ElementType.GetTypeInfo()) as DbProjectionExpression;
        }

        private void GetColumns(MappingEntity entity, Dictionary<string, TableAlias> aliases, List<DbColumnDeclaration> columns)
        {
            foreach (var mi in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsAssociationRelationship(entity, mi) == false)
                {
                    if (this.mapping.IsNestedEntity(entity, mi))
                    {
                        this.GetColumns(this.mapping.GetRelatedEntity(entity, mi), aliases, columns);
                    }
                    else if (this.mapping.IsColumn(entity, mi))
                    {
                        var name = this.mapping.GetColumnName(entity, mi);
                        var aliasName = this.mapping.GetAlias(entity, mi);

                        aliases.TryGetValue(aliasName, out TableAlias alias);

                        var colType = this.GetColumnType(entity, mi);
                        var ce = new DbColumnExpression(TypeHelper.GetMemberType(mi), colType, alias, name);
                        var cd = new DbColumnDeclaration(name, ce, colType);

                        columns.Add(cd);
                    }
                }
            }
        }

        public override Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tables = this.mapping.GetTables(entity);

            if (tables.Count < 2)
            {
                return base.GetInsertExpression(entity, instance, selector);
            }

            var commands = new List<Expression>();

            var map = this.GetDependentGeneratedColumns(entity);
            var vexMap = new Dictionary<MemberInfo, Expression>();

            foreach (var table in this.GetDependencyOrderedTables(entity))
            {
                var tableAlias = new TableAlias();
                var tex = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(table));
                var assignments = this.GetColumnAssignments
                (
                    tex, instance, entity,
                    (e, m) => this.mapping.GetAlias(e, m) == this.mapping.GetAlias(table) && !this.mapping.IsGenerated(e, m),
                    vexMap
                );
                var totalAssignments = assignments.Concat(this.GetRelatedColumnAssignments(tex, entity, table, vexMap));

                commands.Add(new DbInsertCommand(tex, totalAssignments));

                if (map.TryGetValue(this.mapping.GetAlias(table), out List<MemberInfo> members))
                {
                    var d = this.GetDependentGeneratedVariableDeclaration(entity, table, members, instance, vexMap);

                    commands.Add(d);
                }
            }

            if (selector != null)
            {
                commands.Add(this.GetInsertResult(entity, instance, selector, vexMap));
            }

            return new DbBlockCommand(commands);
        }

        private Dictionary<string, List<MemberInfo>> GetDependentGeneratedColumns(MappingEntity entity)
        {
            return
            (
                from xt in this.mapping.GetTables(entity).Where(t => this.mapping.IsExtensionTable(t))
                group xt by this.mapping.GetExtensionRelatedAlias(xt)
            ).ToDictionary
            (
                g => g.Key,
                g => g.SelectMany(xt => this.mapping.GetExtensionRelatedMembers(xt)).Distinct().ToList()
            );
        }

        private DbCommandExpression GetDependentGeneratedVariableDeclaration(MappingEntity entity, MappingTable table, List<MemberInfo> members, Expression instance, Dictionary<MemberInfo, Expression> map)
        {
            var genIdCommand = null as DbDeclarationCommand;
            var generatedIds = this.mapping.GetMappedMembers(entity).Where(m => this.mapping.IsPrimaryKey(entity, m) && this.mapping.IsGenerated(entity, m)).ToList();

            if (generatedIds.Count > 0)
            {
                genIdCommand = this.GetGeneratedIdCommand(entity, members, map);

                if (members.Count == generatedIds.Count)
                {
                    return genIdCommand;
                }
            }

            members = members.Except(generatedIds).ToList();

            var tableAlias = new TableAlias();
            var tex = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(table));
            var where = null as Expression;

            if (generatedIds.Count > 0)
            {
                where = generatedIds.Select((m, i) => this.GetMemberExpression(tex, entity, m).Equal(map[m])).Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance);
            }

            var selectAlias = new TableAlias();
            var columns = new List<DbColumnDeclaration>();
            var variables = new List<DbVariableDeclaration>();

            foreach (var mi in members)
            {
                var col = (DbColumnExpression)this.GetMemberExpression(tex, entity, mi);

                columns.Add(new DbColumnDeclaration(this.mapping.GetColumnName(entity, mi), col, col.QueryType));

                var vcol = new DbColumnExpression(col.Type, col.QueryType, selectAlias, col.Name);

                variables.Add(new DbVariableDeclaration(mi.Name, col.QueryType, vcol));
                map.Add(mi, new DbVariableExpression(mi.Name, col.Type, col.QueryType));
            }

            var genMembersCommand = new DbDeclarationCommand(variables, new DbSelectExpression(selectAlias, columns, tex, where));

            if (genIdCommand != null)
            {
                return new DbBlockCommand(genIdCommand, genMembersCommand);
            }

            return genMembersCommand;
        }

        private IEnumerable<DbColumnAssignment> GetColumnAssignments(Expression table, Expression instance, MappingEntity entity, Func<MappingEntity, MemberInfo, bool> fnIncludeColumn, Dictionary<MemberInfo, Expression> map)
        {
            foreach (var m in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsColumn(entity, m) && fnIncludeColumn(entity, m))
                {
                    yield return new DbColumnAssignment
                    (
                        this.GetMemberExpression(table, entity, m) as DbColumnExpression,
                        this.GetMemberAccess(instance, m, map)
                    );
                }
                else if (this.mapping.IsNestedEntity(entity, m))
                {
                    var assignments = this.GetColumnAssignments
                    (
                        table,
                        Expression.MakeMemberAccess(instance, m),
                        this.mapping.GetRelatedEntity(entity, m),
                        fnIncludeColumn,
                        map
                    );

                    foreach (var ca in assignments)
                    {
                        yield return ca;
                    }
                }
            }
        }

        private IEnumerable<DbColumnAssignment> GetRelatedColumnAssignments(Expression expr, MappingEntity entity, MappingTable table, Dictionary<MemberInfo, Expression> map)
        {
            if (this.mapping.IsExtensionTable(table))
            {
                var keyColumns = this.mapping.GetExtensionKeyColumnNames(table).ToArray();
                var relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToArray();

                for (int i = 0, n = keyColumns.Length; i < n; i++)
                {
                    var member = relatedMembers[i];
                    var exp = map[member];

                    yield return new DbColumnAssignment(this.GetMemberExpression(expr, entity, member) as DbColumnExpression, exp);
                }
            }
        }

        private Expression GetMemberAccess(Expression instance, MemberInfo member, Dictionary<MemberInfo, Expression> map)
        {
            if (map == null || !map.TryGetValue(member, out Expression exp))
            {
                exp = Expression.MakeMemberAccess(instance, member);
            }

            return exp;
        }

        public override Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression exp)
        {
            var tables = this.mapping.GetTables(entity);

            if (tables.Count < 2)
            {
                return base.GetUpdateExpression(entity, instance, updateCheck, selector, exp);
            }

            var commands = new List<Expression>();

            foreach (var table in this.GetDependencyOrderedTables(entity))
            {
                var tex = new DbTableExpression(new TableAlias(), entity, this.mapping.GetTableName(table));
                var assignments = this.GetColumnAssignments(tex, instance, entity, (e, m) => this.mapping.GetAlias(e, m) == this.mapping.GetAlias(table) && this.mapping.IsUpdatable(e, m), null);
                var where = this.GetIdentityCheck(tex, entity, instance);

                commands.Add(new DbUpdateCommand(tex, where, assignments));
            }

            if (selector != null)
            {
                commands.Add(new DbIFCommand
                (
                    this.Translator.Linguist.Language.GetRowsAffectedExpression(commands[commands.Count - 1]).GreaterThan(Expression.Constant(0)),
                    this.GetUpdateResult(entity, instance, selector), exp)
                );
            }
            else if (exp != null)
            {
                commands.Add(new DbIFCommand
                (
                    this.Translator.Linguist.Language.GetRowsAffectedExpression(commands[commands.Count - 1]).LessThanOrEqual(Expression.Constant(0)),
                    exp,
                    null
                ));
            }

            var block = new DbBlockCommand(commands) as Expression;

            if (updateCheck != null)
            {
                return new DbIFCommand(this.GetEntityStateTest(entity, instance, updateCheck), block, null);
            }

            return block;
        }

        private Expression GetIdentityCheck(DbTableExpression root, MappingEntity entity, Expression instance, MappingTable table)
        {
            if (this.mapping.IsExtensionTable(table))
            {
                var keyColNames = this.mapping.GetExtensionKeyColumnNames(table).ToArray();
                var relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToArray();
                var where = null as Expression;

                for (int i = 0, n = keyColNames.Length; i < n; i++)
                {
                    var relatedMember = relatedMembers[i];
                    var cex = new DbColumnExpression(TypeHelper.GetMemberType(relatedMember), this.GetColumnType(entity, relatedMember), root.Alias, keyColNames[n]);
                    var nex = this.GetMemberExpression(instance, entity, relatedMember);
                    var eq = cex.Equal(nex);

                    where = (where != null) ? where.And(eq) : where;
                }

                return where;
            }

            return base.GetIdentityCheck(root, entity, instance);
        }

        public override Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck)
        {
            var commands = new List<Expression>();
            var tables = this.mapping.GetTables(entity);

            if (tables.Count < 2)
            {
                return base.GetDeleteExpression(entity, instance, deleteCheck);
            }

            foreach (var table in this.GetDependencyOrderedTables(entity).Reverse())
            {
                var tex = new DbTableExpression(new TableAlias(), entity, this.mapping.GetTableName(table));
                var where = this.GetIdentityCheck(tex, entity, instance);

                commands.Add(new DbDeleteCommand(tex, where));
            }

            var block = new DbBlockCommand(commands) as Expression;

            if (deleteCheck != null)
            {
                return new DbIFCommand(this.GetEntityStateTest(entity, instance, deleteCheck), block, null);
            }

            return block;
        }
    }
}