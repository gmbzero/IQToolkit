using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal abstract class BasicMapping : QueryMapping
    {
        public override MappingEntity GetEntity(Type elementType, string tableId)
        {
            if (tableId == null)
            {
                tableId = this.GetTableId(elementType);
            }

            return new BasicMappingEntity(elementType, tableId);
        }

        public override MappingEntity GetEntity(MemberInfo contextMember)
        {
            var elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(contextMember));

            return this.GetEntity(elementType);
        }

        private class BasicMappingEntity : MappingEntity
        {
            public override string TableId { get; }

            public override Type ElementType { get; }

            public override Type EntityType { get; }

            public BasicMappingEntity(Type type, string entityID)
            {
                this.TableId = entityID;
                this.ElementType = type;
                this.EntityType = type;
            }
        }

        public override bool IsRelationship(MappingEntity entity, MemberInfo member)
        {
            return this.IsAssociationRelationship(entity, member);
        }

        public virtual bool IsMapped(MappingEntity entity, MemberInfo member)
        {
            return true;
        }

        public virtual bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            return this.IsMapped(entity, member);
        }

        public virtual string GetColumnDbType(MappingEntity entity, MemberInfo member)
        {
            return null;
        }

        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        public virtual bool IsComputed(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        public virtual bool IsGenerated(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        public virtual bool IsReadOnly(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        public virtual bool IsUpdatable(MappingEntity entity, MemberInfo member)
        {
            return !this.IsPrimaryKey(entity, member) && !this.IsReadOnly(entity, member);
        }

        public virtual MappingEntity GetRelatedEntity(MappingEntity entity, MemberInfo member)
        {
            var relatedType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));

            return this.GetEntity(relatedType);
        }

        public virtual bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        public virtual IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            return new MemberInfo[] { };
        }

        public virtual IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            return new MemberInfo[] { };
        }

        public abstract bool IsRelationshipSource(MappingEntity entity, MemberInfo member);

        public abstract bool IsRelationshipTarget(MappingEntity entity, MemberInfo member);

        public virtual string GetTableName(MappingEntity entity)
        {
            return entity.EntityType.Name;
        }

        public virtual string GetColumnName(MappingEntity entity, MemberInfo member)
        {
            return member.Name;
        }

        public override IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity)
        {
            var type = entity.EntityType;
            var members = new HashSet<MemberInfo>(type.GetFields().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));

            members.UnionWith(type.GetProperties().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));

            return members.OrderBy(m => m.Name);
        }

        public override object CloneEntity(MappingEntity entity, object instance)
        {
            var clone = Expression.Lambda<Func<object>>(Expression.New(entity.ElementType), null).Compile()();

            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsColumn(entity, mi))
                {
                    mi.SetValue(clone, mi.GetValue(instance));
                }
            }

            return clone;
        }

        public override bool IsModified(MappingEntity entity, object instance, object original)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsColumn(entity, mi))
                {
                    if (object.Equals(mi.GetValue(instance), mi.GetValue(original)) == false)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public override object GetPrimaryKey(MappingEntity entity, object instance)
        {
            var firstKey = null as object;
            var keys = null as List<object>;

            foreach (var mi in this.GetPrimaryKeyMembers(entity))
            {
                if (firstKey == null)
                {
                    firstKey = mi.GetValue(instance);
                }
                else
                {
                    if (keys == null)
                    {
                        keys = new List<object>
                        {
                            firstKey
                        };
                    }

                    keys.Add(mi.GetValue(instance));
                }
            }

            if (keys != null)
            {
                return new CompoundKey(keys.ToArray());
            }

            return firstKey;
        }

        public override Expression GetPrimaryKeyQuery(MappingEntity entity, Expression source, Expression[] keys)
        {
            var p = Expression.Parameter(entity.ElementType, "p");
            var pred = null as Expression;

            var idMembers = this.GetPrimaryKeyMembers(entity).ToList();

            if (idMembers.Count != keys.Length)
            {
                throw new InvalidOperationException("Incorrect number of primary key values");
            }

            for (int i = 0, n = keys.Length; i < n; i++)
            {
                var mem = idMembers[i];
                var memberType = TypeHelper.GetMemberType(mem);

                if (keys[i] != null && TypeHelper.GetNonNullableType(keys[i].Type) != TypeHelper.GetNonNullableType(memberType))
                {
                    throw new InvalidOperationException("Primary key value is wrong type");
                }

                var eq = Expression.MakeMemberAccess(p, mem).Equal(keys[i]);

                pred = (pred == null) ? eq : pred.And(eq);
            }

            var predLambda = Expression.Lambda(pred, p);

            return Expression.Call(typeof(Queryable), "SingleOrDefault", new Type[] { entity.ElementType }, source, predLambda);
        }

        public override IEnumerable<EntityInfo> GetDependentEntities(MappingEntity entity, object instance)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsRelationship(entity, mi) && this.IsRelationshipSource(entity, mi))
                {
                    var relatedEntity = this.GetRelatedEntity(entity, mi);
                    var value = mi.GetValue(instance);

                    if (value != null)
                    {
                        if (value is IList list)
                        {
                            foreach (var item in list)
                            {
                                if (item != null)
                                {
                                    yield return new EntityInfo(item, relatedEntity);
                                }
                            }
                        }
                        else
                        {
                            yield return new EntityInfo(value, relatedEntity);
                        }
                    }
                }
            }
        }

        public override IEnumerable<EntityInfo> GetDependingEntities(MappingEntity entity, object instance)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsRelationship(entity, mi) && this.IsRelationshipTarget(entity, mi))
                {
                    var relatedEntity = this.GetRelatedEntity(entity, mi);
                    var value = mi.GetValue(instance);

                    if (value != null)
                    {
                        if (value is IList list)
                        {
                            foreach (var item in list)
                            {
                                if (item != null)
                                {
                                    yield return new EntityInfo(item, relatedEntity);
                                }
                            }
                        }
                        else
                        {
                            yield return new EntityInfo(value, relatedEntity);
                        }
                    }
                }
            }
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new BasicMapper(this, translator);
        }
    }

    internal class BasicMapper : QueryMapper
    {
        private readonly BasicMapping mapping;
        private readonly QueryTranslator translator;

        public override QueryMapping Mapping
        {
            get { return this.mapping; }
        }

        public override QueryTranslator Translator
        {
            get { return this.translator; }
        }

        public BasicMapper(BasicMapping mapping, QueryTranslator translator)
        {
            this.mapping = mapping;
            this.translator = translator;
        }

        public virtual QueryType GetColumnType(MappingEntity entity, MemberInfo member)
        {
            var dbType = this.mapping.GetColumnDbType(entity, member);

            if (dbType != null)
            {
                return this.translator.Linguist.Language.TypeSystem.Parse(dbType);
            }

            return this.translator.Linguist.Language.TypeSystem.GetColumnType(TypeHelper.GetMemberType(member));
        }

        internal override DbProjectionExpression GetQueryExpression(MappingEntity entity)
        {
            var tableAlias = new TableAlias();
            var selectAlias = new TableAlias();
            var table = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(entity));

            var projector = this.GetEntityExpression(table, entity) as Expression;
            var pc = DbColumnProjector.ProjectColumns(this.translator.Linguist.Language, projector, null, selectAlias, tableAlias);

            var proj = new DbProjectionExpression(new DbSelectExpression(selectAlias, pc.Columns, table, null), pc.Projector);

            return this.Translator.Police.ApplyPolicy(proj, entity.ElementType.GetTypeInfo()) as DbProjectionExpression;
        }

        internal override DbEntityExpression GetEntityExpression(Expression root, MappingEntity entity)
        {
            var assignments = new List<EntityAssignment>();

            foreach (var mi in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsAssociationRelationship(entity, mi) == false)
                {
                    var me = this.GetMemberExpression(root, entity, mi);

                    if (me != null)
                    {
                        assignments.Add(new EntityAssignment(mi, me));
                    }
                }
            }

            return new DbEntityExpression(entity, BuildEntityExpression(entity, assignments));
        }

        public class EntityAssignment
        {
            public MemberInfo Member { get; }
            public Expression Expression { get; }

            public EntityAssignment(MemberInfo member, Expression expression)
            {
                this.Member = member;
                this.Expression = expression;
            }
        }

        protected virtual Expression BuildEntityExpression(MappingEntity entity, IList<EntityAssignment> assignments)
        {
            var newExpression = null as NewExpression;
            var readonlyMembers = assignments.Where(b => TypeHelper.IsReadOnly(b.Member)).ToArray();
            var cons = entity.EntityType.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            var hasNoArgConstructor = cons.Any(c => c.GetParameters().Length == 0);

            if (readonlyMembers.Length > 0 || !hasNoArgConstructor)
            {
                var consThatApply = cons.Select(c => this.BindConstructor(c, readonlyMembers)).Where(cbr => cbr != null && cbr.Remaining.Count == 0).ToList();

                if (consThatApply.Count == 0)
                {
                    throw new InvalidOperationException($"Cannot construct type '{ entity.ElementType }' with all mapped includedMembers");
                }

                if (readonlyMembers.Length == assignments.Count)
                {
                    return consThatApply[0].Expression;
                }

                var r = this.BindConstructor(consThatApply[0].Expression.Constructor, assignments);

                newExpression = r.Expression;
                assignments = r.Remaining;
            }
            else
            {
                newExpression = Expression.New(entity.EntityType);
            }

            var result = null as Expression;

            if (assignments.Count > 0)
            {
                if (entity.ElementType.GetTypeInfo().IsInterface)
                {
                    assignments = this.MapAssignments(assignments, entity.EntityType).ToList();
                }

                result = Expression.MemberInit(newExpression, assignments.Select(a => Expression.Bind(a.Member, a.Expression)).ToArray() as MemberBinding[]);
            }
            else
            {
                result = newExpression;
            }

            if (entity.ElementType != entity.EntityType)
            {
                result = Expression.Convert(result, entity.ElementType);
            }

            return result;
        }

        private IEnumerable<EntityAssignment> MapAssignments(IEnumerable<EntityAssignment> assignments, Type entityType)
        {
            foreach (var assign in assignments)
            {
                var members = entityType.GetMember(assign.Member.Name, BindingFlags.Instance | BindingFlags.Public);

                if (members != null && members.Length > 0)
                {
                    yield return new EntityAssignment(members[0], assign.Expression);
                }
                else
                {
                    yield return assign;
                }
            }
        }

        protected virtual ConstructorBindResult BindConstructor(ConstructorInfo cons, IList<EntityAssignment> assignments)
        {
            var ps = cons.GetParameters();
            var args = new Expression[ps.Length];
            var mis = new MemberInfo[ps.Length];
            var members = new HashSet<EntityAssignment>(assignments);
            var used = new HashSet<EntityAssignment>();

            for (int i = 0, n = ps.Length; i < n; i++)
            {
                var p = ps[i];
                var assignment = members.FirstOrDefault(a => p.Name == a.Member.Name && p.ParameterType.IsAssignableFrom(a.Expression.Type));

                if (assignment == null)
                {
                    assignment = members.FirstOrDefault(a => string.Compare(p.Name, a.Member.Name, true) == 0 && p.ParameterType.IsAssignableFrom(a.Expression.Type));
                }

                if (assignment != null)
                {
                    args[i] = assignment.Expression;

                    if (mis != null)
                    {
                        mis[i] = assignment.Member;
                    }

                    used.Add(assignment);
                }
                else
                {
                    var mems = cons.DeclaringType.GetMember(p.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);

                    if (mems != null && mems.Length > 0)
                    {
                        args[i] = Expression.Constant(TypeHelper.GetDefault(p.ParameterType), p.ParameterType);

                        mis[i] = mems[0];
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            members.ExceptWith(used);

            return new ConstructorBindResult(Expression.New(cons, args, mis), members);
        }

        protected class ConstructorBindResult
        {
            public NewExpression Expression { get; }

            public ReadOnlyCollection<EntityAssignment> Remaining { get; }

            public ConstructorBindResult(NewExpression expression, IEnumerable<EntityAssignment> remaining)
            {
                this.Expression = expression;
                this.Remaining = remaining.ToReadOnly();
            }
        }

        internal override bool HasIncludedMembers(DbEntityExpression entity)
        {
            var policy = this.translator.Police.Policy;

            foreach (var mi in this.mapping.GetMappedMembers(entity.Entity))
            {
                if (policy.IsIncluded(mi))
                {
                    return true;
                }
            }

            return false;
        }

        internal override DbEntityExpression IncludeMembers(DbEntityExpression entity, Func<MemberInfo, bool> fnIsIncluded)
        {
            var assignments = this.GetAssignments(entity.Expression).ToDictionary(ma => ma.Member.Name);
            var anyAdded = false;

            foreach (var mi in this.mapping.GetMappedMembers(entity.Entity))
            {
                var okayToInclude = !assignments.TryGetValue(mi.Name, out EntityAssignment ea) || IsNullRelationshipAssignment(entity.Entity, ea);

                if (okayToInclude && fnIsIncluded(mi))
                {
                    ea = new EntityAssignment(mi, this.GetMemberExpression(entity.Expression, entity.Entity, mi));
                    assignments[mi.Name] = ea;
                    anyAdded = true;
                }
            }

            if (anyAdded)
            {
                return new DbEntityExpression(entity.Entity, this.BuildEntityExpression(entity.Entity, assignments.Values.ToList()));
            }

            return entity;
        }

        private bool IsNullRelationshipAssignment(MappingEntity entity, EntityAssignment assignment)
        {
            if (this.mapping.IsRelationship(entity, assignment.Member))
            {
                if (assignment.Expression is ConstantExpression cex && cex.Value == null)
                {
                    return true;
                }
            }

            return false;
        }

        private IEnumerable<EntityAssignment> GetAssignments(Expression newOrMemberInit)
        {
            var assignments = new List<EntityAssignment>();

            if (newOrMemberInit is MemberInitExpression minit)
            {
                assignments.AddRange(minit.Bindings.OfType<MemberAssignment>().Select(a => new EntityAssignment(a.Member, a.Expression)));

                newOrMemberInit = minit.NewExpression;
            }

            if (newOrMemberInit is NewExpression nex && nex.Members != null)
            {
                assignments.AddRange(Enumerable.Range(0, nex.Arguments.Count).Where(i => nex.Members[i] != null).Select(i => new EntityAssignment(nex.Members[i], nex.Arguments[i])));
            }

            return assignments;
        }

        public override Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member)
        {
            if (this.mapping.IsAssociationRelationship(entity, member))
            {
                var relatedEntity = this.mapping.GetRelatedEntity(entity, member);
                var projection = this.GetQueryExpression(relatedEntity);
                var declaredTypeMembers = this.mapping.GetAssociationKeyMembers(entity, member).ToList();
                var associatedMembers = this.mapping.GetAssociationRelatedKeyMembers(entity, member).ToList();

                var where = null as Expression;

                for (int i = 0, n = associatedMembers.Count; i < n; i++)
                {
                    var equal = this.GetMemberExpression
                    (
                        projection.Projector,
                        relatedEntity,
                        associatedMembers[i]).Equal(this.GetMemberExpression(root, entity, declaredTypeMembers[i])
                    );

                    where = (where != null) ? where.And(equal) : equal;
                }

                var newAlias = new TableAlias();
                var pc = DbColumnProjector.ProjectColumns
                (
                    this.translator.Linguist.Language,
                    projection.Projector, null,
                    newAlias,
                    projection.Select.Alias
                );

                var aggregator = DbAggregator.GetAggregator(TypeHelper.GetMemberType(member), typeof(IEnumerable<>).MakeGenericType(pc.Projector.Type));

                var result = new DbProjectionExpression
                (
                    new DbSelectExpression(newAlias, pc.Columns, projection.Select, where),
                    pc.Projector, aggregator
                );

                return this.translator.Police.ApplyPolicy(result, member);
            }

            if (root is DbAliasedExpression aliasedRoot && this.mapping.IsColumn(entity, member))
            {
                return new DbColumnExpression
                (
                    TypeHelper.GetMemberType(member),
                    this.GetColumnType(entity, member),
                    aliasedRoot.Alias,
                    this.mapping.GetColumnName(entity, member)
                );
            }

            return DbQueryBinder.BindMember(root, member);
        }

        public override Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tableAlias = new TableAlias();
            var table = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(entity));
            var assignments = this.GetColumnAssignments(table, instance, entity, (e, m) => !(mapping.IsGenerated(e, m) || mapping.IsReadOnly(e, m)));

            if (selector != null)
            {
                return new DbBlockCommand
                (
                    new DbInsertCommand(table, assignments),
                    this.GetInsertResult(entity, instance, selector, null)
                );
            }

            return new DbInsertCommand(table, assignments);
        }

        private IEnumerable<DbColumnAssignment> GetColumnAssignments(Expression table, Expression instance, MappingEntity entity, Func<MappingEntity, MemberInfo, bool> fnIncludeColumn)
        {
            foreach (var m in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsColumn(entity, m) && fnIncludeColumn(entity, m))
                {
                    yield return new DbColumnAssignment
                    (
                        this.GetMemberExpression(table, entity, m) as DbColumnExpression,
                        Expression.MakeMemberAccess(instance, m)
                    );
                }
            }
        }

        protected virtual Expression GetInsertResult(MappingEntity entity, Expression instance, LambdaExpression selector, Dictionary<MemberInfo, Expression> map)
        {
            var tableAlias = new TableAlias();
            var tex = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(entity));
            var aggregator = DbAggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type));

            var where = null as Expression;
            var genIdCommand = null as DbDeclarationCommand;
            var generatedIds = this.mapping.GetMappedMembers(entity).Where(m => this.mapping.IsPrimaryKey(entity, m) && this.mapping.IsGenerated(entity, m)).ToList();

            if (generatedIds.Count > 0)
            {
                if (map == null || !generatedIds.Any(m => map.ContainsKey(m)))
                {
                    var localMap = new Dictionary<MemberInfo, Expression>();
                    genIdCommand = this.GetGeneratedIdCommand(entity, generatedIds.ToList(), localMap);
                    map = localMap;
                }

                if (selector.Body is MemberExpression mex && this.mapping.IsPrimaryKey(entity, mex.Member) && this.mapping.IsGenerated(entity, mex.Member))
                {
                    if (genIdCommand != null)
                    {
                        return new DbProjectionExpression
                        (
                            genIdCommand.Source,
                            new DbColumnExpression(mex.Type, genIdCommand.Variables[0].QueryType, genIdCommand.Source.Alias, genIdCommand.Source.Columns[0].Name),
                            aggregator
                        );
                    }
                    else
                    {
                        var alias = new TableAlias();
                        var colType = this.GetColumnType(entity, mex.Member);

                        return new DbProjectionExpression
                        (
                            new DbSelectExpression(alias, new[] { new DbColumnDeclaration(string.Empty, map[mex.Member], colType) }, null, null),
                            new DbColumnExpression(TypeHelper.GetMemberType(mex.Member), colType, alias, string.Empty),
                            aggregator
                        );
                    }
                }

                where = generatedIds.Select((m, i) => this.GetMemberExpression(tex, entity, m).Equal(map[m])).Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance);
            }

            var typeProjector = this.GetEntityExpression(tex, entity);
            var selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], typeProjector);
            var newAlias = new TableAlias();
            var pc = DbColumnProjector.ProjectColumns(this.translator.Linguist.Language, selection, null, newAlias, tableAlias);
            var pe = new DbProjectionExpression
            (
                new DbSelectExpression(newAlias, pc.Columns, tex, where),
                pc.Projector,
                aggregator
            );

            if (genIdCommand != null)
            {
                return new DbBlockCommand(genIdCommand, pe);
            }

            return pe;
        }

        internal virtual DbDeclarationCommand GetGeneratedIdCommand(MappingEntity entity, List<MemberInfo> members, Dictionary<MemberInfo, Expression> map)
        {
            var columns = new List<DbColumnDeclaration>();
            var decls = new List<DbVariableDeclaration>();
            var alias = new TableAlias();

            foreach (var member in members)
            {
                var genId = this.translator.Linguist.Language.GetGeneratedIdExpression(member);
                var name = member.Name;
                var colType = this.GetColumnType(entity, member);

                columns.Add(new DbColumnDeclaration(member.Name, genId, colType));
                decls.Add(new DbVariableDeclaration(member.Name, colType, new DbColumnExpression(genId.Type, colType, alias, member.Name)));

                if (map != null)
                {
                    map.Add(member, new DbVariableExpression(member.Name, TypeHelper.GetMemberType(member), colType));
                }
            }

            return new DbDeclarationCommand(decls, new DbSelectExpression(alias, columns, null, null));
        }

        protected virtual Expression GetIdentityCheck(Expression root, MappingEntity entity, Expression instance)
        {
            return this.mapping.GetMappedMembers(entity).Where(m => this.mapping.IsPrimaryKey(entity, m))
                               .Select(m => this.GetMemberExpression(root, entity, m).Equal(Expression.MakeMemberAccess(instance, m)))
                               .Aggregate((x, y) => x.And(y));
        }

        protected virtual Expression GetEntityExistsTest(MappingEntity entity, Expression instance)
        {
            var tq = this.GetQueryExpression(entity);
            var where = this.GetIdentityCheck(tq.Select, entity, instance);

            return new DbExistsExpression(new DbSelectExpression(new TableAlias(), null, tq.Select, where));
        }

        protected virtual Expression GetEntityStateTest(MappingEntity entity, Expression instance, LambdaExpression updateCheck)
        {
            var tq = this.GetQueryExpression(entity);
            var where = this.GetIdentityCheck(tq.Select, entity, instance);
            var check = DbExpressionReplacer.Replace(updateCheck.Body, updateCheck.Parameters[0], tq.Projector);

            if (check != null)
            {
                where = where.And(check);
            }

            return new DbExistsExpression(new DbSelectExpression(new TableAlias(), null, tq.Select, where));
        }

        public override Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression exp)
        {
            var tableAlias = new TableAlias();
            var table = new DbTableExpression(tableAlias, entity, this.mapping.GetTableName(entity));
            var where = this.GetIdentityCheck(table, entity, instance);

            if (updateCheck != null)
            {
                var typeProjector = this.GetEntityExpression(table, entity);
                var pred = DbExpressionReplacer.Replace(updateCheck.Body, updateCheck.Parameters[0], typeProjector);

                if (pred != null)
                {
                    where = where.And(pred);
                }
            }

            var assignments = this.GetColumnAssignments(table, instance, entity, (e, m) => this.mapping.IsUpdatable(e, m));

            var update = new DbUpdateCommand(table, where, assignments);

            if (selector != null)
            {
                return new DbBlockCommand
                (
                    update,
                    new DbIFCommand
                    (
                        this.translator.Linguist.Language.GetRowsAffectedExpression(update).GreaterThan(Expression.Constant(0)),
                        this.GetUpdateResult(entity, instance, selector),
                        exp
                     )
                );
            }
            else if (exp != null)
            {
                return new DbBlockCommand
                (
                    update,
                    new DbIFCommand
                    (
                        this.translator.Linguist.Language.GetRowsAffectedExpression(update).LessThanOrEqual(Expression.Constant(0)),
                        exp,
                        null
                    )
                );
            }
            else
            {
                return update;
            }
        }

        protected virtual Expression GetUpdateResult(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tq = this.GetQueryExpression(entity);
            var where = this.GetIdentityCheck(tq.Select, entity, instance);
            var selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], tq.Projector);
            var newAlias = new TableAlias();
            var pc = DbColumnProjector.ProjectColumns(this.translator.Linguist.Language, selection, null, newAlias, tq.Select.Alias);

            return new DbProjectionExpression
            (
                new DbSelectExpression(newAlias, pc.Columns, tq.Select, where),
                pc.Projector,
                DbAggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type))
            );
        }

        public override Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            if (updateCheck != null)
            {
                var insert = this.GetInsertExpression(entity, instance, resultSelector);
                var update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, null);
                var check = this.GetEntityExistsTest(entity, instance);

                return new DbIFCommand(check, update, insert);
            }
            else
            {
                var insert = this.GetInsertExpression(entity, instance, resultSelector);
                var update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, insert);

                return update;
            }
        }

        public override Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck)
        {
            var table = new DbTableExpression(new TableAlias(), entity, this.mapping.GetTableName(entity));
            var where = null as Expression;

            if (instance != null)
            {
                where = this.GetIdentityCheck(table, entity, instance);
            }

            if (deleteCheck != null)
            {
                var row = this.GetEntityExpression(table, entity);
                var pred = DbExpressionReplacer.Replace(deleteCheck.Body, deleteCheck.Parameters[0], row);

                if (pred != null)
                {
                    where = (where != null) ? where.And(pred) : pred;
                }
            }

            return new DbDeleteCommand(table, where);
        }
    }
}