using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal class DbExecutionBuilder : DbExpressionVisitor
    {
        private Expression executor;
        private Scope scope;
        private bool isTop = true;
        private MemberInfo receivingMember;
        private int nReaders = 0;
        private Dictionary<string, Expression> variableMap;
        private readonly List<ParameterExpression> variables;
        private readonly List<Expression> initializers;
        private readonly QueryPolicy policy;
        private readonly QueryLinguist linguist;

        private DbExecutionBuilder(QueryLinguist linguist, QueryPolicy policy, Expression executor)
        {
            this.linguist = linguist;
            this.policy = policy;
            this.executor = executor;
            this.variables = new List<ParameterExpression>();
            this.initializers = new List<Expression>();
            this.variableMap = new Dictionary<string, Expression>();
        }

        public static Expression Build(QueryLinguist linguist, QueryPolicy policy, Expression expression, Expression provider)
        {
            var executor = Expression.Parameter(typeof(QueryExecutor), "executor");
            var builder = new DbExecutionBuilder(linguist, policy, executor);

            if (builder != null)
            {
                builder.variables.Add(executor);
                builder.initializers.Add(Expression.Call(Expression.Convert(provider, typeof(ICreateExecutor)), nameof(ICreateExecutor.CreateExecutor), null, null));
            }

            return builder.Build(expression);
        }

        private Expression Build(Expression expression)
        {
            expression = this.Visit(expression);
            expression = this.AddVariables(expression);

            return expression;
        }

        private Expression AddVariables(Expression expression)
        {
            if (this.variables.Count > 0)
            {
                var exprs = new List<Expression>();

                for (int i = 0, n = this.variables.Count; i < n; i++)
                {
                    exprs.Add(MakeAssign(this.variables[i], this.initializers[i]));
                }

                exprs.Add(expression);

                var sequence = MakeSequence(exprs);

                var nulls = this.variables.Select(v => Expression.Constant(null, v.Type)).ToArray();

                expression = Expression.Invoke(Expression.Lambda(sequence, this.variables.ToArray()), nulls);
            }

            return expression;
        }

        private static Expression MakeSequence(IList<Expression> expressions)
        {
            var last = expressions[expressions.Count - 1];

            expressions = expressions.Select(e => e.Type.GetTypeInfo().IsValueType ? Expression.Convert(e, typeof(object)) : e).ToList();

            return Expression.Convert(Expression.Call(typeof(DbExecutionBuilder), nameof(DbExecutionBuilder.Sequence), null, Expression.NewArrayInit(typeof(object), expressions)), last.Type);
        }

        public static object Sequence(params object[] values)
        {
            return values[values.Length - 1];
        }

        public static IEnumerable<R> Batch<T, R>(IEnumerable<T> items, Func<T, R> selector, bool stream)
        {
            var result = items.Select(selector);

            if (stream == false)
            {
                return result.ToList();
            }

            return new EnumerateOnce<R>(result);
        }

        private static Expression MakeAssign(ParameterExpression variable, Expression value)
        {
            return Expression.Call(typeof(DbExecutionBuilder), nameof(DbExecutionBuilder.Assign), new Type[] { variable.Type }, variable, value);
        }

        public static T Assign<T>(ref T variable, T value)
        {
            variable = value;

            return value;
        }

        private Expression BuildInner(Expression expression)
        {
            var eb = new DbExecutionBuilder(this.linguist, this.policy, this.executor)
            {
                scope = this.scope,
                receivingMember = this.receivingMember,
                nReaders = this.nReaders,
                nLookup = this.nLookup,
                variableMap = this.variableMap
            };

            return eb.Build(expression);
        }

        protected override MemberBinding VisitBinding(MemberBinding binding)
        {
            var save = this.receivingMember;

            this.receivingMember = binding.Member;

            var result = base.VisitBinding(binding);

            this.receivingMember = save;

            return result;
        }

        int nLookup = 0;

        private Expression MakeJoinKey(IList<Expression> key)
        {
            if (key.Count == 1)
            {
                return key[0];
            }

            return Expression.New
            (
                typeof(CompoundKey).GetConstructors()[0],
                Expression.NewArrayInit(typeof(object), key.Select(k => Expression.Convert(k, typeof(object)) as Expression))
            );
        }

        protected override Expression VisitClientJoin(DbClientJoinExpression join)
        {
            var innerKey = MakeJoinKey(join.InnerKey);
            var outerKey = MakeJoinKey(join.OuterKey);

            var kvpConstructor = typeof(KeyValuePair<,>).MakeGenericType(innerKey.Type, join.Projection.Projector.Type).GetConstructor(new Type[] { innerKey.Type, join.Projection.Projector.Type });
            var constructKVPair = Expression.New(kvpConstructor, innerKey, join.Projection.Projector);
            var newProjection = new DbProjectionExpression(join.Projection.Select, constructKVPair);

            var iLookup = ++nLookup;
            var execution = this.ExecuteProjection(newProjection, okayToDefer: false, isTopLevel: false);

            var kvp = Expression.Parameter(constructKVPair.Type, "kvp");

            if (join.Projection.Projector.NodeType == (ExpressionType)DbExpressionType.OuterJoined)
            {
                var pred = Expression.Lambda
                (
                    Expression.PropertyOrField(kvp, "Value").NotEqual(TypeHelper.GetNullConstant(join.Projection.Projector.Type)),
                    kvp
                );

                execution = Expression.Call(typeof(Enumerable), nameof(Enumerable.Where), new Type[] { kvp.Type }, execution, pred);
            }

            var keySelector = Expression.Lambda(Expression.PropertyOrField(kvp, "Key"), kvp);
            var elementSelector = Expression.Lambda(Expression.PropertyOrField(kvp, "Value"), kvp);
            var toLookup = Expression.Call(typeof(Enumerable), nameof(Enumerable.ToLookup), new Type[] { kvp.Type, outerKey.Type, join.Projection.Projector.Type }, execution, keySelector, elementSelector);

            var lookup = Expression.Parameter(toLookup.Type, "lookup" + iLookup);
            var property = lookup.Type.GetProperty("Item");
            var access = Expression.Call(lookup, property.GetGetMethod(), this.Visit(outerKey)) as Expression;

            if (join.Projection.Aggregator != null)
            {
                access = DbExpressionReplacer.Replace(join.Projection.Aggregator.Body, join.Projection.Aggregator.Parameters[0], access);
            }

            this.variables.Add(lookup);
            this.initializers.Add(toLookup);

            return access;
        }

        protected override Expression VisitProjection(DbProjectionExpression projection)
        {
            if (this.isTop)
            {
                this.isTop = false;

                return this.ExecuteProjection(projection, okayToDefer: this.scope != null, isTopLevel: true);
            }

            return this.BuildInner(projection);
        }

        protected virtual Expression Parameterize(Expression expression)
        {
            if (this.variableMap.Count > 0)
            {
                expression = VariableSubstitutor.Substitute(this.variableMap, expression);
            }

            return this.linguist.Parameterize(expression);
        }

        private Expression ExecuteProjection(DbProjectionExpression projection, bool okayToDefer, bool isTopLevel)
        {
            projection = this.Parameterize(projection) as DbProjectionExpression;

            if (this.scope != null)
            {
                projection = OuterParameterizer.Parameterize(this.scope.Alias, projection) as DbProjectionExpression;
            }

            var commandText = this.linguist.Format(projection.Select);
            var namedValues = DbNamedValueGatherer.Gather(projection.Select);
            var command = new QueryCommand(commandText, namedValues.Select(v => new QueryParameter(v.Name, v.Type, v.QueryType)));
            var values = namedValues.Select(v => Expression.Convert(this.Visit(v.Value), typeof(object))).ToArray() as Expression[];

            return this.ExecuteProjection(projection, okayToDefer, command, values, isTopLevel);
        }

        private Expression ExecuteProjection(DbProjectionExpression projection, bool okayToDefer, QueryCommand command, Expression[] values, bool isTopLevel)
        {
            okayToDefer &= this.receivingMember != null && this.policy.IsDeferLoaded(this.receivingMember);

            var saveScope = this.scope;
            var reader = Expression.Parameter(typeof(FieldReader), "r" + nReaders++);

            this.scope = new Scope(this.scope, reader, projection.Select.Alias, projection.Select.Columns);

            var projector = Expression.Lambda(this.Visit(projection.Projector), reader);

            this.scope = saveScope;

            var entity = EntityFinder.Find(projection.Projector);

            var methExecute = okayToDefer ? nameof(QueryExecutor.ExecuteDeferred) : nameof(QueryExecutor.Execute);

            var result = Expression.Call(this.executor, methExecute, new Type[] { projector.Body.Type }, Expression.Constant(command), projector, Expression.Constant(entity, typeof(MappingEntity)), Expression.NewArrayInit(typeof(object), values)) as Expression;

            if (projection.Aggregator != null)
            {
                result = DbExpressionReplacer.Replace(projection.Aggregator.Body, projection.Aggregator.Parameters[0], result);
            }

            return result;
        }

        protected override Expression VisitBatch(DbBatchExpression batch)
        {
            if (this.linguist.Language.AllowsMultipleCommands || !IsMultipleCommands(batch.Operation.Body as DbCommandExpression))
            {
                return this.BuildExecuteBatch(batch);
            }

            var source = this.Visit(batch.Input);
            var op = this.Visit(batch.Operation.Body);
            var fn = Expression.Lambda(op, batch.Operation.Parameters[1]);

            return Expression.Call(typeof(DbExecutionBuilder), nameof(DbExecutionBuilder.Batch), new Type[] { TypeHelper.GetElementType(source.Type), batch.Operation.Body.Type }, source, fn, batch.Stream);
        }

        protected virtual Expression BuildExecuteBatch(DbBatchExpression batch)
        {
            var operation = this.Parameterize(batch.Operation.Body);
            var commandText = this.linguist.Format(operation);
            var namedValues = DbNamedValueGatherer.Gather(operation);
            var command = new QueryCommand(commandText, namedValues.Select(v => new QueryParameter(v.Name, v.Type, v.QueryType)));
            var values = namedValues.Select(v => Expression.Convert(this.Visit(v.Value), typeof(object))).ToArray() as Expression[];

            var paramSets = Expression.Call
            (
                typeof(Enumerable), nameof(Enumerable.Select), new Type[] { batch.Operation.Parameters[1].Type, typeof(object[]) },
                batch.Input,
                Expression.Lambda(Expression.NewArrayInit(typeof(object), values), new[] { batch.Operation.Parameters[1] })
            );

            var plan = null as Expression;
            var projection = ProjectionFinder.FindProjection(operation);

            if (projection != null)
            {
                var saveScope = this.scope;
                var reader = Expression.Parameter(typeof(FieldReader), "r" + nReaders++);

                this.scope = new Scope(this.scope, reader, projection.Select.Alias, projection.Select.Columns);

                var projector = Expression.Lambda(this.Visit(projection.Projector), reader);

                this.scope = saveScope;

                var entity = EntityFinder.Find(projection.Projector);

                command = new QueryCommand(command.CommandText, command.Parameters);

                plan = Expression.Call
                (
                    this.executor, nameof(QueryExecutor.ExecuteBatch), new Type[] { projector.Body.Type },
                    Expression.Constant(command),
                    paramSets,
                    projector,
                    Expression.Constant(entity, typeof(MappingEntity)),
                    batch.BatchSize,
                    batch.Stream
                );
            }
            else
            {
                plan = Expression.Call
                (
                    this.executor, nameof(QueryExecutor.ExecuteBatch), null,
                    Expression.Constant(command),
                    paramSets,
                    batch.BatchSize,
                    batch.Stream
                );
            }

            return plan;
        }

        protected override Expression VisitCommand(DbCommandExpression command)
        {
            if (this.linguist.Language.AllowsMultipleCommands || !IsMultipleCommands(command))
            {
                return this.BuildExecuteCommand(command);
            }

            return base.VisitCommand(command);
        }

        protected virtual bool IsMultipleCommands(DbCommandExpression command)
        {
            if (command == null)
            {
                return false;
            }

            switch ((DbExpressionType)command.NodeType)
            {
                case DbExpressionType.Insert:
                case DbExpressionType.Delete:
                case DbExpressionType.Update: return false;
                default: return true;
            }
        }

        protected override Expression VisitInsert(DbInsertCommand insert)
        {
            return this.BuildExecuteCommand(insert);
        }

        protected override Expression VisitUpdate(DbUpdateCommand update)
        {
            return this.BuildExecuteCommand(update);
        }

        protected override Expression VisitDelete(DbDeleteCommand delete)
        {
            return this.BuildExecuteCommand(delete);
        }

        protected override Expression VisitBlock(DbBlockCommand block)
        {
            return MakeSequence(this.VisitExpressionList(block.Commands));
        }

        protected override Expression VisitIf(DbIFCommand ifx)
        {
            var test = Expression.Condition
            (
                ifx.Check,
                ifx.IfTrue,
                ifx.IfFalse ?? (ifx.IfTrue.Type == typeof(int) ? Expression.Property(this.executor, nameof(QueryExecutor.RowsAffected)) as Expression : Expression.Constant(TypeHelper.GetDefault(ifx.IfTrue.Type), ifx.IfTrue.Type) as Expression)
             );

            return this.Visit(test);
        }

        protected override Expression VisitFunction(DbFunctionExpression func)
        {
            if (this.linguist.Language.IsRowsAffectedExpressions(func))
            {
                return Expression.Property(this.executor, nameof(QueryExecutor.RowsAffected));
            }

            return base.VisitFunction(func);
        }

        protected override Expression VisitExists(DbExistsExpression exists)
        {
            var colType = this.linguist.Language.TypeSystem.GetColumnType(typeof(int));

            var newSelect = exists.Select.SetColumns(new[]
            {
                new DbColumnDeclaration("value", new DbAggregateExpression(typeof(int), "Count", null, false), colType)
            });

            var projection = new DbProjectionExpression
            (
                newSelect,
                new DbColumnExpression(typeof(int), colType, newSelect.Alias, "value"),
                DbAggregator.GetAggregator(typeof(int), typeof(IEnumerable<int>))
            );

            return this.Visit(projection.GreaterThan(Expression.Constant(0)));
        }

        protected override Expression VisitDeclaration(DbDeclarationCommand decl)
        {
            if (decl.Source != null)
            {
                var projection = new DbProjectionExpression
                (
                    decl.Source,
                    Expression.NewArrayInit(typeof(object), decl.Variables.Select(v => v.Expression.Type.GetTypeInfo().IsValueType ? Expression.Convert(v.Expression, typeof(object)) : v.Expression).ToArray()),
                    DbAggregator.GetAggregator(typeof(object[]), typeof(IEnumerable<object[]>))
                );

                var vars = Expression.Parameter(typeof(object[]), "vars");

                this.variables.Add(vars);
                this.initializers.Add(Expression.Constant(null, typeof(object[])));

                for (int i = 0, n = decl.Variables.Count; i < n; i++)
                {
                    var v = decl.Variables[i];
                    var nv = new DbNamedValueExpression
                    (
                        v.Name, v.QueryType,
                        Expression.Convert(Expression.ArrayIndex(vars, Expression.Constant(i)), v.Expression.Type)
                    );

                    this.variableMap.Add(v.Name, nv);
                }

                return MakeAssign(vars, this.Visit(projection));
            }

            throw new InvalidOperationException("Declaration query not allowed for this langauge");
        }

        protected virtual Expression BuildExecuteCommand(DbCommandExpression command)
        {
            var expression = this.Parameterize(command);
            var commandText = this.linguist.Format(expression);
            var namedValues = DbNamedValueGatherer.Gather(expression);
            var qc = new QueryCommand(commandText, namedValues.Select(v => new QueryParameter(v.Name, v.Type, v.QueryType)));
            var values = namedValues.Select(v => Expression.Convert(this.Visit(v.Value), typeof(object))).ToArray() as Expression[];

            var projection = ProjectionFinder.FindProjection(expression);

            if (projection != null)
            {
                return this.ExecuteProjection(projection, false, qc, values, isTopLevel: true);
            }

            var plan = Expression.Call
            (
                this.executor, nameof(QueryExecutor.ExecuteCommand), null,
                Expression.Constant(qc),
                Expression.NewArrayInit(typeof(object), values)
            );

            return plan;
        }

        protected override Expression VisitEntity(DbEntityExpression entity)
        {
            return this.Visit(entity.Expression);
        }

        protected override Expression VisitOuterJoined(DbOuterJoinedExpression outer)
        {
            var expr = this.Visit(outer.Expression);
            var column = outer.Test as DbColumnExpression;

            if (this.scope.TryGetValue(column, out ParameterExpression reader, out int iOrdinal))
            {
                return Expression.Condition
                (
                    Expression.Call(reader, "IsDbNull", null, Expression.Constant(iOrdinal)),
                    Expression.Constant(TypeHelper.GetDefault(outer.Type), outer.Type),
                    expr
                );
            }

            return expr;
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.scope != null && this.scope.TryGetValue(column, out ParameterExpression fieldReader, out int iOrdinal))
            {
                var method = FieldReader.GetReaderMethod(column.Type);

                return Expression.Call(fieldReader, method, Expression.Constant(iOrdinal));
            }
            else
            {
                Debug.Fail(string.Format("column not in scope: {0}", column));
            }

            return column;
        }

        private class Scope
        {
            private readonly Scope outer;
            private readonly ParameterExpression fieldReader;
            private readonly Dictionary<string, int> nameMap;

            public TableAlias Alias { get; }

            public Scope(Scope outer, ParameterExpression fieldReader, TableAlias alias, IEnumerable<DbColumnDeclaration> columns)
            {
                this.outer = outer;
                this.fieldReader = fieldReader;
                this.Alias = alias;
                this.nameMap = columns.Select((c, i) => new { c, i }).ToDictionary(x => x.c.Name, x => x.i);
            }

            public bool TryGetValue(DbColumnExpression column, out ParameterExpression fieldReader, out int ordinal)
            {
                for (var s = this; s != null; s = s.outer)
                {
                    if (column.Alias == s.Alias && this.nameMap.TryGetValue(column.Name, out ordinal))
                    {
                        fieldReader = this.fieldReader;

                        return true;
                    }
                }

                fieldReader = null;
                ordinal = 0;

                return false;
            }
        }

        private class OuterParameterizer : DbExpressionVisitor
        {
            private int iParam;
            private TableAlias outerAlias;
            private readonly Dictionary<DbColumnExpression, DbNamedValueExpression> map;

            private OuterParameterizer()
            {
                this.map = new Dictionary<DbColumnExpression, DbNamedValueExpression>();
            }

            public static Expression Parameterize(TableAlias outerAlias, Expression expr)
            {
                var op = new OuterParameterizer
                {
                    outerAlias = outerAlias
                };

                return op.Visit(expr);
            }

            protected override Expression VisitProjection(DbProjectionExpression proj)
            {
                var select = this.Visit(proj.Select) as DbSelectExpression;

                return this.UpdateProjection(proj, select, proj.Projector, proj.Aggregator);
            }

            protected override Expression VisitColumn(DbColumnExpression column)
            {
                if (column.Alias == this.outerAlias)
                {
                    if (!this.map.TryGetValue(column, out DbNamedValueExpression nv))
                    {
                        nv = new DbNamedValueExpression("n" + (iParam++), column.QueryType, column);

                        this.map.Add(column, nv);
                    }

                    return nv;
                }

                return column;
            }
        }

        private class ColumnGatherer : DbExpressionVisitor
        {
            private readonly Dictionary<string, DbColumnExpression> columns;

            private ColumnGatherer()
            {
                this.columns = new Dictionary<string, DbColumnExpression>();
            }

            public static IEnumerable<DbColumnExpression> Gather(Expression expression)
            {
                var gatherer = new ColumnGatherer();

                if (gatherer != null)
                {
                    gatherer.Visit(expression);
                }

                return gatherer.columns.Values;
            }

            protected override Expression VisitColumn(DbColumnExpression column)
            {
                if (!this.columns.ContainsKey(column.Name))
                {
                    this.columns.Add(column.Name, column);
                }
                return column;
            }
        }

        private class ProjectionFinder : DbExpressionVisitor
        {
            private DbProjectionExpression found = null;

            public static DbProjectionExpression FindProjection(Expression expression)
            {
                var finder = new ProjectionFinder();

                if (finder != null)
                {
                    finder.Visit(expression);
                }

                return finder.found;
            }

            protected override Expression VisitProjection(DbProjectionExpression proj)
            {
                this.found = proj;

                return proj;
            }
        }

        private class VariableSubstitutor : DbExpressionVisitor
        {
            private readonly Dictionary<string, Expression> map;

            private VariableSubstitutor(Dictionary<string, Expression> map)
            {
                this.map = map;
            }

            public static Expression Substitute(Dictionary<string, Expression> map, Expression expression)
            {
                return new VariableSubstitutor(map).Visit(expression);
            }

            protected override Expression VisitVariable(DbVariableExpression vex)
            {
                if (this.map.TryGetValue(vex.Name, out Expression sub))
                {
                    return sub;
                }

                return vex;
            }
        }

        private class EntityFinder : DbExpressionVisitor
        {
            private MappingEntity entity;

            public static MappingEntity Find(Expression expression)
            {
                var finder = new EntityFinder();

                if (finder != null)
                {
                    finder.Visit(expression);
                }

                return finder.entity;
            }

            protected override Expression Visit(Expression exp)
            {
                if (entity == null)
                {
                    return base.Visit(exp);
                }

                return exp;
            }

            protected override Expression VisitEntity(DbEntityExpression entity)
            {
                if (this.entity == null)
                {
                    this.entity = entity.Entity;
                }

                return entity;
            }

            protected override NewExpression VisitNew(NewExpression nex)
            {
                return nex;
            }

            protected override Expression VisitMemberInit(MemberInitExpression init)
            {
                return init;
            }
        }
    }
}