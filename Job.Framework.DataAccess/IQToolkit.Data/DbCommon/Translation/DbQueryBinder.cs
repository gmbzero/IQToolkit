using Job.Framework.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal class DbQueryBinder : DbExpressionVisitor
    {
        private Expression root;
        private Expression currentGroupElement;
        private IEntity batchUpd;
        private List<DbOrderExpression> thenBys;
        private readonly QueryMapper mapper;
        private readonly QueryLanguage language;
        private readonly Dictionary<ParameterExpression, Expression> map;
        private readonly Dictionary<Expression, GroupByInfo> groupByMap;

        private DbQueryBinder(QueryMapper mapper, Expression root)
        {
            this.mapper = mapper;
            this.language = mapper.Translator.Linguist.Language;
            this.map = new Dictionary<ParameterExpression, Expression>();
            this.groupByMap = new Dictionary<Expression, GroupByInfo>();
            this.root = root;
        }

        public static Expression Bind(QueryMapper mapper, Expression expression)
        {
            return new DbQueryBinder(mapper, expression).Visit(expression);
        }

        private static LambdaExpression GetLambda(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = (e as UnaryExpression).Operand;
            }

            if (e.NodeType == ExpressionType.Constant)
            {
                return (e as ConstantExpression).Value as LambdaExpression;
            }

            return e as LambdaExpression;
        }

        public TableAlias GetNextAlias()
        {
            return new TableAlias();
        }

        private ProjectedColumns ProjectColumns(Expression expression, TableAlias newAlias, params TableAlias[] existingAliases)
        {
            return DbColumnProjector.ProjectColumns(this.language, expression, null, newAlias, existingAliases);
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Queryable) || m.Method.DeclaringType == typeof(Enumerable))
            {
                switch (m.Method.Name)
                {
                    case "Where":
                        {
                            return this.BindWhere(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]));
                        }
                    case "Select":
                        {
                            return this.BindSelect(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]));
                        }
                    case "SelectMany":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindSelectMany(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]), null);
                            }
                            else if (m.Arguments.Count == 3)
                            {
                                return this.BindSelectMany(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]), GetLambda(m.Arguments[2]));
                            }

                            break;
                        }
                    case "Join":
                        {
                            return this.BindJoin(m.Type, m.Arguments[0], m.Arguments[1], GetLambda(m.Arguments[2]), GetLambda(m.Arguments[3]), GetLambda(m.Arguments[4]));
                        }
                    case "GroupJoin":
                        {
                            if (m.Arguments.Count == 5)
                            {
                                return this.BindGroupJoin(m.Method, m.Arguments[0], m.Arguments[1], GetLambda(m.Arguments[2]), GetLambda(m.Arguments[3]), GetLambda(m.Arguments[4]));
                            }
                            break;
                        }
                    case "OrderBy":
                        {
                            return this.BindOrderBy(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]), OrderType.Ascending);
                        }
                    case "OrderByDescending":
                        {
                            return this.BindOrderBy(m.Type, m.Arguments[0], GetLambda(m.Arguments[1]), OrderType.Descending);
                        }
                    case "ThenBy":
                        {
                            return this.BindThenBy(m.Arguments[0], GetLambda(m.Arguments[1]), OrderType.Ascending);
                        }
                    case "ThenByDescending":
                        {
                            return this.BindThenBy(m.Arguments[0], GetLambda(m.Arguments[1]), OrderType.Descending);
                        }
                    case "GroupBy":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindGroupBy(m.Arguments[0], GetLambda(m.Arguments[1]), null, null);
                            }
                            else if (m.Arguments.Count == 3)
                            {
                                var lambda1 = GetLambda(m.Arguments[1]);
                                var lambda2 = GetLambda(m.Arguments[2]);

                                if (lambda2.Parameters.Count == 1)
                                {
                                    return this.BindGroupBy(m.Arguments[0], lambda1, lambda2, null);
                                }
                                else if (lambda2.Parameters.Count == 2)
                                {
                                    return this.BindGroupBy(m.Arguments[0], lambda1, null, lambda2);
                                }
                            }
                            else if (m.Arguments.Count == 4)
                            {
                                return this.BindGroupBy(m.Arguments[0], GetLambda(m.Arguments[1]), GetLambda(m.Arguments[2]), GetLambda(m.Arguments[3]));
                            }

                            break;
                        }
                    case "Distinct":
                        {
                            if (m.Arguments.Count == 1)
                            {
                                return this.BindDistinct(m.Arguments[0]);
                            }

                            break;
                        }
                    case "Skip":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindSkip(m.Arguments[0], m.Arguments[1]);
                            }
                            break;
                        }
                    case "Take":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindTake(m.Arguments[0], m.Arguments[1]);
                            }

                            break;
                        }
                    case "First":
                    case "FirstOrDefault":
                    case "Single":
                    case "SingleOrDefault":
                    case "Last":
                    case "LastOrDefault":
                        {
                            if (m.Arguments.Count == 1)
                            {
                                return this.BindFirst(m.Arguments[0], null, m.Method.Name, m == this.root);
                            }
                            else if (m.Arguments.Count == 2)
                            {
                                return this.BindFirst(m.Arguments[0], GetLambda(m.Arguments[1]), m.Method.Name, m == this.root);
                            }
                            break;
                        }
                    case "Any":
                        {
                            if (m.Arguments.Count == 1)
                            {
                                return this.BindAnyAll(m.Arguments[0], m.Method, null, m == this.root);
                            }
                            else if (m.Arguments.Count == 2)
                            {
                                return this.BindAnyAll(m.Arguments[0], m.Method, GetLambda(m.Arguments[1]), m == this.root);
                            }

                            break;
                        }
                    case "All":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindAnyAll(m.Arguments[0], m.Method, GetLambda(m.Arguments[1]), m == this.root);
                            }
                            break;
                        }
                    case "Contains":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindContains(m.Arguments[0], m.Arguments[1], m == this.root);
                            }
                            break;
                        }
                    case "Cast":
                        {
                            if (m.Arguments.Count == 1)
                            {
                                return this.BindCast(m.Arguments[0], m.Method.GetGenericArguments()[0]);
                            }
                            break;
                        }
                    case "Reverse":
                        {
                            return this.BindReverse(m.Arguments[0]);
                        }
                    case "Intersect":
                    case "Except":
                        {
                            if (m.Arguments.Count == 2)
                            {
                                return this.BindIntersect(m.Arguments[0], m.Arguments[1], m.Method.Name == "Except");
                            }

                            break;
                        }
                }
            }
            else if (typeof(QueryUpdatable).IsAssignableFrom(m.Method.DeclaringType))
            {
                var upd = this.batchUpd != null ? this.batchUpd : (m.Arguments[0] as ConstantExpression).Value as IEntity;

                switch (m.Method.Name)
                {
                    case "Insert":
                        {
                            return this.BindInsert
                            (
                                upd,
                                m.Arguments[1],
                                m.Arguments.Count > 2 ? GetLambda(m.Arguments[2]) : null
                            );
                        }
                    case "Update":
                        {
                            return this.BindUpdate
                            (
                                upd,
                                m.Arguments[1],
                                m.Arguments.Count > 2 ? GetLambda(m.Arguments[2]) : null,
                                m.Arguments.Count > 3 ? GetLambda(m.Arguments[3]) : null
                            );
                        }
                    case "InsertOrUpdate":
                        {
                            return this.BindInsertOrUpdate
                            (
                                upd,
                                m.Arguments[1],
                                m.Arguments.Count > 2 ? GetLambda(m.Arguments[2]) : null,
                                m.Arguments.Count > 3 ? GetLambda(m.Arguments[3]) : null
                            );
                        }
                    case "Delete":
                        {
                            if (m.Arguments.Count == 2 && GetLambda(m.Arguments[1]) != null)
                            {
                                return this.BindDelete(upd, null, GetLambda(m.Arguments[1]));
                            }

                            return this.BindDelete
                            (
                                upd,
                                m.Arguments[1],
                                m.Arguments.Count > 2 ? GetLambda(m.Arguments[2]) : null
                            );
                        }
                    case "Batch":
                        {
                            return this.BindBatch
                            (
                                upd,
                                m.Arguments[1],
                                GetLambda(m.Arguments[2]),
                                m.Arguments.Count > 3 ? m.Arguments[3] : Expression.Constant(50),
                                m.Arguments.Count > 4 ? m.Arguments[4] : Expression.Constant(false)
                            );
                        }
                }
            }

            if (this.language.IsAggregate(m.Method))
            {
                return this.BindAggregate
                (
                    m.Arguments[0],
                    m.Method.Name,
                    m.Method.ReturnType,
                    m.Arguments.Count > 1 ? GetLambda(m.Arguments[1]) : null,
                    m == this.root
                );
            }

            return base.VisitMethodCall(m);
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            if ((u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked) && u == this.root)
            {
                this.root = u.Operand;
            }

            return base.VisitUnary(u);
        }

        private DbProjectionExpression VisitSequence(Expression source)
        {
            return this.ConvertToSequence(base.Visit(source));
        }

        private DbProjectionExpression ConvertToSequence(Expression expr)
        {
            switch (expr.NodeType)
            {
                case (ExpressionType)DbExpressionType.Projection:
                    {
                        return expr as DbProjectionExpression;
                    }
                case ExpressionType.New:
                    {
                        var nex = expr as NewExpression;

                        if (expr.Type.GetTypeInfo().IsGenericType && expr.Type.GetGenericTypeDefinition() == typeof(Grouping<,>))
                        {
                            return nex.Arguments[1] as DbProjectionExpression;
                        }

                        goto default;
                    }
                case ExpressionType.MemberAccess:
                    {
                        var bound = this.BindRelationshipProperty(expr as MemberExpression);

                        if (bound.NodeType != ExpressionType.MemberAccess)
                        {
                            return this.ConvertToSequence(bound);
                        }

                        goto default;
                    }
                default:
                    {
                        var n = this.GetNewExpression(expr);

                        if (n != null)
                        {
                            expr = n;

                            goto case ExpressionType.New;
                        }

                        throw new Exception($"The expression of type '{ expr.Type }' is not a sequence");
                    }
            }
        }

        private Expression BindRelationshipProperty(MemberExpression mex)
        {
            if (mex.Expression is DbEntityExpression ex && this.mapper.Mapping.IsRelationship(ex.Entity, mex.Member))
            {
                return this.mapper.GetMemberExpression(mex.Expression, ex.Entity, mex.Member);
            }

            return mex;
        }

        protected override Expression Visit(Expression exp)
        {
            var result = base.Visit(exp);

            if (result != null)
            {
                var expectedType = exp.Type;

                if (result is DbProjectionExpression projection && projection.Aggregator == null && !expectedType.IsAssignableFrom(projection.Type))
                {
                    var aggregator = DbAggregator.GetAggregator(expectedType, projection.Type);

                    if (aggregator != null)
                    {
                        return new DbProjectionExpression(projection.Select, projection.Projector, aggregator);
                    }
                }
            }

            return result;
        }

        private Expression BindWhere(Type resultType, Expression source, LambdaExpression predicate)
        {
            var projection = this.VisitSequence(source);

            this.map[predicate.Parameters[0]] = projection.Projector;

            var where = this.Visit(predicate.Body);
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression
            (
                new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    projection.Select,
                    where
                ),
                pc.Projector
            );
        }

        private Expression BindReverse(Expression source)
        {
            var projection = this.VisitSequence(source);
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression
            (
                new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    projection.Select,
                    null
                ).SetReverse(true),
                pc.Projector
            );
        }

        private Expression BindSelect(Type resultType, Expression source, LambdaExpression selector)
        {
            var projection = this.VisitSequence(source);

            this.map[selector.Parameters[0]] = projection.Projector;

            var expression = this.Visit(selector.Body);
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(expression, alias, projection.Select.Alias);

            return new DbProjectionExpression
            (
                new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    projection.Select,
                    null
                ),
                pc.Projector
            );
        }

        protected virtual Expression BindSelectMany(Type resultType, Expression source, LambdaExpression collectionSelector, LambdaExpression resultSelector)
        {
            var projection = this.VisitSequence(source);

            this.map[collectionSelector.Parameters[0]] = projection.Projector;

            var collection = collectionSelector.Body;
            var defaultIfEmpty = false;

            if (collection is MethodCallExpression mcs && mcs.Method.Name == "DefaultIfEmpty" && mcs.Arguments.Count == 1 && (mcs.Method.DeclaringType == typeof(Queryable) || mcs.Method.DeclaringType == typeof(Enumerable)))
            {
                collection = mcs.Arguments[0];
                defaultIfEmpty = true;
            }

            var collectionProjection = this.VisitSequence(collection);
            var isTable = collectionProjection.Select.From is DbTableExpression;
            var joinType = isTable ? JoinType.CrossJoin : defaultIfEmpty ? JoinType.OuterApply : JoinType.CrossApply;

            if (joinType == JoinType.OuterApply)
            {
                collectionProjection = this.language.AddOuterJoinTest(collectionProjection);
            }

            var join = new DbJoinExpression(joinType, projection.Select, collectionProjection.Select, null);

            var alias = this.GetNextAlias();
            var pc = null as ProjectedColumns;

            if (resultSelector == null)
            {
                pc = this.ProjectColumns(collectionProjection.Projector, alias, projection.Select.Alias, collectionProjection.Select.Alias);
            }
            else
            {
                this.map[resultSelector.Parameters[0]] = projection.Projector;
                this.map[resultSelector.Parameters[1]] = collectionProjection.Projector;

                pc = this.ProjectColumns(this.Visit(resultSelector.Body), alias, projection.Select.Alias, collectionProjection.Select.Alias);
            }

            return new DbProjectionExpression
            (
                new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    join,
                    null
                ),
                pc.Projector
            );
        }

        protected virtual Expression BindJoin(Type resultType, Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector)
        {
            var outerProjection = this.VisitSequence(outerSource);
            var innerProjection = this.VisitSequence(innerSource);

            this.map[outerKey.Parameters[0]] = outerProjection.Projector;

            var outerKeyExpr = this.Visit(outerKey.Body);

            this.map[innerKey.Parameters[0]] = innerProjection.Projector;

            var innerKeyExpr = this.Visit(innerKey.Body);

            this.map[resultSelector.Parameters[0]] = outerProjection.Projector;
            this.map[resultSelector.Parameters[1]] = innerProjection.Projector;

            var resultExpr = this.Visit(resultSelector.Body);
            var join = new DbJoinExpression(JoinType.InnerJoin, outerProjection.Select, innerProjection.Select, outerKeyExpr.Equal(innerKeyExpr));
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(resultExpr, alias, outerProjection.Select.Alias, innerProjection.Select.Alias);

            return new DbProjectionExpression
            (
                new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    join,
                    null
                ),
                pc.Projector
            );
        }

        protected virtual Expression BindIntersect(Expression outerSource, Expression innerSource, bool negate)
        {
            var outerProjection = this.VisitSequence(outerSource);
            var innerProjection = this.VisitSequence(innerSource);

            var exists = (Expression)new DbExistsExpression(new DbSelectExpression
            (
                new TableAlias(),
                null,
                innerProjection.Select,
                innerProjection.Projector.Equal(outerProjection.Projector)
            ));

            if (negate)
            {
                exists = Expression.Not(exists);
            }

            var alias = this.GetNextAlias();

            var pc = this.ProjectColumns(outerProjection.Projector, alias, outerProjection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                outerProjection.Select,
                exists
            ), pc.Projector, outerProjection.Aggregator);
        }

        protected virtual Expression BindGroupJoin(MethodInfo groupJoinMethod, Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector)
        {
            var args = groupJoinMethod.GetGenericArguments();
            var outerProjection = this.VisitSequence(outerSource);

            this.map[outerKey.Parameters[0]] = outerProjection.Projector;

            var predicateLambda = Expression.Lambda(innerKey.Body.Equal(outerKey.Body), innerKey.Parameters[0]);
            var callToWhere = Expression.Call(typeof(Enumerable), "Where", new Type[] { args[1] }, innerSource, predicateLambda);

            var group = this.Visit(callToWhere);

            this.map[resultSelector.Parameters[0]] = outerProjection.Projector;
            this.map[resultSelector.Parameters[1]] = group;

            var resultExpr = this.Visit(resultSelector.Body);
            var alias = this.GetNextAlias();

            var pc = this.ProjectColumns(resultExpr, alias, outerProjection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                outerProjection.Select, null
            ), pc.Projector);
        }

        protected virtual Expression BindOrderBy(Type resultType, Expression source, LambdaExpression orderSelector, OrderType orderType)
        {
            var myThenBys = this.thenBys;

            this.thenBys = null;

            var projection = this.VisitSequence(source);

            this.map[orderSelector.Parameters[0]] = projection.Projector;

            var orderings = new List<DbOrderExpression>
            {
                new DbOrderExpression(orderType, this.Visit(orderSelector.Body))
            };

            if (myThenBys != null)
            {
                for (var i = myThenBys.Count - 1; i >= 0; i--)
                {
                    var tb = myThenBys[i];
                    var lambda = (LambdaExpression)tb.Expression;

                    this.map[lambda.Parameters[0]] = projection.Projector;

                    orderings.Add(new DbOrderExpression(tb.OrderType, this.Visit(lambda.Body)));
                }
            }

            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                projection.Select,
                null,
                orderings.AsReadOnly(),
                null
            ), pc.Projector);
        }

        protected virtual Expression BindThenBy(Expression source, LambdaExpression orderSelector, OrderType orderType)
        {
            if (this.thenBys == null)
            {
                this.thenBys = new List<DbOrderExpression>();
            }

            this.thenBys.Add(new DbOrderExpression(orderType, orderSelector));

            return this.Visit(source);
        }

        protected virtual Expression BindGroupBy(Expression source, LambdaExpression keySelector, LambdaExpression elementSelector, LambdaExpression resultSelector)
        {
            var projection = this.VisitSequence(source);

            this.map[keySelector.Parameters[0]] = projection.Projector;

            var keyExpr = this.Visit(keySelector.Body);
            var elemExpr = projection.Projector;

            if (elementSelector != null)
            {
                this.map[elementSelector.Parameters[0]] = projection.Projector;

                elemExpr = this.Visit(elementSelector.Body);
            }

            var keyProjection = this.ProjectColumns(keyExpr, projection.Select.Alias, projection.Select.Alias);
            var groupExprs = keyProjection.Columns.Select(c => c.Expression).ToArray();
            var subqueryBasis = this.VisitSequence(source);

            this.map[keySelector.Parameters[0]] = subqueryBasis.Projector;

            var subqueryKey = this.Visit(keySelector.Body);

            var subqueryKeyPC = this.ProjectColumns(subqueryKey, subqueryBasis.Select.Alias, subqueryBasis.Select.Alias);
            var subqueryGroupExprs = subqueryKeyPC.Columns.Select(c => c.Expression).ToArray();
            var subqueryCorrelation = this.BuildPredicateWithNullsEqual(subqueryGroupExprs, groupExprs);

            var subqueryElemExpr = subqueryBasis.Projector;

            if (elementSelector != null)
            {
                this.map[elementSelector.Parameters[0]] = subqueryBasis.Projector;

                subqueryElemExpr = this.Visit(elementSelector.Body);
            }

            var elementAlias = this.GetNextAlias();
            var elementPC = this.ProjectColumns(subqueryElemExpr, elementAlias, subqueryBasis.Select.Alias);

            var elementSubquery = new DbProjectionExpression(new DbSelectExpression
            (
                elementAlias,
                elementPC.Columns,
                subqueryBasis.Select,
                subqueryCorrelation
            ), elementPC.Projector);

            var alias = this.GetNextAlias();
            var info = new GroupByInfo(alias, elemExpr);

            this.groupByMap.Add(elementSubquery, info);

            var resultExpr = null as Expression;

            if (resultSelector != null)
            {
                var saveGroupElement = this.currentGroupElement;

                this.currentGroupElement = elementSubquery;
                this.map[resultSelector.Parameters[0]] = keyProjection.Projector;
                this.map[resultSelector.Parameters[1]] = elementSubquery;

                resultExpr = this.Visit(resultSelector.Body);

                this.currentGroupElement = saveGroupElement;
            }
            else
            {
                resultExpr = Expression.New
                (
                    typeof(Grouping<,>).MakeGenericType(keyExpr.Type, subqueryElemExpr.Type).GetConstructors()[0],
                    new Expression[] { keyExpr, elementSubquery }
                );

                resultExpr = Expression.Convert(resultExpr, typeof(IGrouping<,>).MakeGenericType(keyExpr.Type, subqueryElemExpr.Type));
            }

            var pc = this.ProjectColumns(resultExpr, alias, projection.Select.Alias);
            var newResult = this.GetNewExpression(pc.Projector);

            if (newResult != null && newResult.Type.GetTypeInfo().IsGenericType && newResult.Type.GetGenericTypeDefinition() == typeof(Grouping<,>))
            {
                var projectedElementSubquery = newResult.Arguments[1];

                this.groupByMap.Add(projectedElementSubquery, info);
            }

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                projection.Select,
                null, null, groupExprs
            ), pc.Projector);
        }

        private NewExpression GetNewExpression(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
            {
                expression = (expression as UnaryExpression).Operand;
            }

            return expression as NewExpression;
        }

        private Expression BuildPredicateWithNullsEqual(IEnumerable<Expression> source1, IEnumerable<Expression> source2)
        {
            var en1 = source1.GetEnumerator();
            var en2 = source2.GetEnumerator();

            var result = null as Expression;

            while (en1.MoveNext() && en2.MoveNext())
            {
                var compare = Expression.Or
                (
                    new DbIsNullExpression(en1.Current).And(new DbIsNullExpression(en2.Current)),
                    en1.Current.Equal(en2.Current)
                );

                result = (result == null) ? compare : result.And(compare);
            }

            return result;
        }

        private class GroupByInfo
        {
            public TableAlias Alias { get; }

            public Expression Element { get; }

            public GroupByInfo(TableAlias alias, Expression element)
            {
                this.Alias = alias;
                this.Element = element;
            }
        }

        private Expression BindAggregate(Expression source, string aggName, Type returnType, LambdaExpression argument, bool isRoot)
        {
            var hasPredicateArg = this.language.AggregateArgumentIsPredicate(aggName);
            var isDistinct = false;
            var argumentWasPredicate = false;
            var useAlternateArg = false;

            if (source is MethodCallExpression mcs && !hasPredicateArg && argument == null)
            {
                if (mcs.Method.Name == "Distinct" && mcs.Arguments.Count == 1 && (mcs.Method.DeclaringType == typeof(Queryable) || mcs.Method.DeclaringType == typeof(Enumerable)) && this.language.AllowDistinctInAggregates)
                {
                    source = mcs.Arguments[0];
                    isDistinct = true;
                }
            }

            if (argument != null && hasPredicateArg)
            {
                source = Expression.Call(typeof(Queryable), "Where", new[] { TypeHelper.GetElementType(source.Type) }, source, argument);
                argument = null;
                argumentWasPredicate = true;
            }

            var projection = this.VisitSequence(source);

            var argExpr = null as Expression;

            if (argument != null)
            {
                this.map[argument.Parameters[0]] = projection.Projector;

                argExpr = this.Visit(argument.Body);
            }
            else if (hasPredicateArg == false || useAlternateArg)
            {
                argExpr = projection.Projector;
            }

            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);
            var aggExpr = new DbAggregateExpression(returnType, aggName, argExpr, isDistinct);
            var colType = this.language.TypeSystem.GetColumnType(returnType);
            var select = new DbSelectExpression(alias, new DbColumnDeclaration[] { new DbColumnDeclaration(string.Empty, aggExpr, colType) }, projection.Select, null);

            if (isRoot)
            {
                var p = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(aggExpr.Type), "p");
                var gator = Expression.Lambda(Expression.Call(typeof(Enumerable), "Single", new Type[] { returnType }, p), p);

                return new DbProjectionExpression(select, new DbColumnExpression(returnType, this.language.TypeSystem.GetColumnType(returnType), alias, string.Empty), gator);
            }

            var subquery = new DbScalarExpression(returnType, select);

            if (argumentWasPredicate == false && this.groupByMap.TryGetValue(projection, out GroupByInfo info))
            {
                if (argument != null)
                {
                    this.map[argument.Parameters[0]] = info.Element;

                    argExpr = this.Visit(argument.Body);
                }
                else if (!hasPredicateArg || useAlternateArg)
                {
                    argExpr = info.Element;
                }

                aggExpr = new DbAggregateExpression(returnType, aggName, argExpr, isDistinct);

                if (projection == this.currentGroupElement)
                {
                    return aggExpr;
                }

                return new DbAggregateSubqueryExpression(info.Alias, aggExpr, subquery);
            }

            return subquery;
        }

        private Expression BindDistinct(Expression source)
        {
            var projection = this.VisitSequence(source);
            var select = projection.Select;
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                projection.Select,
                null, null, null, true, null, null, false
            ), pc.Projector);
        }

        private Expression BindTake(Expression source, Expression take)
        {
            var projection = this.VisitSequence(source);

            take = this.Visit(take);

            var select = projection.Select;
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                projection.Select,
                null, null, null, false, null,
                take, false
            ), pc.Projector);
        }

        private Expression BindSkip(Expression source, Expression skip)
        {
            var projection = this.VisitSequence(source);
            skip = this.Visit(skip);
            var select = projection.Select;
            var alias = this.GetNextAlias();
            var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

            return new DbProjectionExpression(new DbSelectExpression
            (
                alias,
                pc.Columns,
                projection.Select,
                null, null, null, false,
                skip, null, false
            ), pc.Projector);
        }

        private Expression BindCast(Expression source, Type targetElementType)
        {
            var projection = this.VisitSequence(source);
            var elementType = this.GetTrueUnderlyingType(projection.Projector);

            if (targetElementType.IsAssignableFrom(elementType) == false)
            {
                throw new InvalidOperationException($"Cannot cast elements from type '{ elementType }' to type '{ targetElementType }'");
            }

            return projection;
        }

        private Type GetTrueUnderlyingType(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert)
            {
                expression = (expression as UnaryExpression).Operand;
            }

            return expression.Type;
        }

        private Expression BindFirst(Expression source, LambdaExpression predicate, string kind, bool isRoot)
        {
            var projection = this.VisitSequence(source);
            var where = null as Expression;

            if (predicate != null)
            {
                this.map[predicate.Parameters[0]] = projection.Projector;

                where = this.Visit(predicate.Body);
            }

            var isFirst = kind.StartsWith("First");
            var isLast = kind.StartsWith("Last");
            var take = (isFirst || isLast) ? Expression.Constant(1) : null;

            if (take != null || where != null)
            {
                var alias = this.GetNextAlias();
                var pc = this.ProjectColumns(projection.Projector, alias, projection.Select.Alias);

                projection = new DbProjectionExpression(new DbSelectExpression
                (
                    alias,
                    pc.Columns,
                    projection.Select,
                    where,
                    null, null, false, null,
                    take, isLast
                ), pc.Projector);
            }

            if (isRoot)
            {
                var elementType = projection.Projector.Type;
                var p = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(elementType), "p");
                var gator = Expression.Lambda(Expression.Call(typeof(Enumerable), kind, new Type[] { elementType }, p), p);

                return new DbProjectionExpression(projection.Select, projection.Projector, gator);
            }

            return projection;
        }

        private Expression BindAnyAll(Expression source, MethodInfo method, LambdaExpression predicate, bool isRoot)
        {
            var isAll = method.Name == "All";

            if (source is ConstantExpression constSource && !IsQuery(constSource))
            {
                var where = null as Expression;

                foreach (var value in (IEnumerable)constSource.Value)
                {
                    var expr = Expression.Invoke(predicate, Expression.Constant(value, predicate.Parameters[0].Type));

                    if (where == null)
                    {
                        where = expr;
                    }
                    else if (isAll)
                    {
                        where = where.And(expr);
                    }
                    else
                    {
                        where = where.Or(expr);
                    }
                }

                return this.Visit(where);
            }
            else
            {
                if (isAll)
                {
                    predicate = Expression.Lambda(Expression.Not(predicate.Body), predicate.Parameters.ToArray());
                }

                if (predicate != null)
                {
                    source = Expression.Call(typeof(Enumerable), "Where", method.GetGenericArguments(), source, predicate);
                }

                var projection = this.VisitSequence(source);
                var result = new DbExistsExpression(projection.Select) as Expression;

                if (isAll)
                {
                    result = Expression.Not(result);
                }

                if (isRoot)
                {
                    if (this.language.AllowSubqueryInSelectWithoutFrom)
                    {
                        return GetSingletonSequence(result, "SingleOrDefault");
                    }
                    else
                    {
                        var colType = this.language.TypeSystem.GetColumnType(typeof(int));
                        var newSelect = projection.Select.SetColumns(new[] { new DbColumnDeclaration("value", new DbAggregateExpression(typeof(int), "Count", null, false), colType) });
                        var colx = new DbColumnExpression(typeof(int), colType, newSelect.Alias, "value");
                        var exp = isAll ? colx.Equal(Expression.Constant(0)) : colx.GreaterThan(Expression.Constant(0));

                        return new DbProjectionExpression(newSelect, exp, DbAggregator.GetAggregator(typeof(bool), typeof(IEnumerable<bool>)));
                    }
                }

                return result;
            }
        }

        private Expression BindContains(Expression source, Expression match, bool isRoot)
        {
            if (source is ConstantExpression constSource && !IsQuery(constSource))
            {
                var values = new List<Expression>();

                foreach (var value in (IEnumerable)constSource.Value)
                {
                    values.Add(Expression.Constant(Convert.ChangeType(value, match.Type), match.Type));
                }

                match = this.Visit(match);

                return new DbInExpression(match, values);
            }
            else if (isRoot && !this.language.AllowSubqueryInSelectWithoutFrom)
            {
                var p = Expression.Parameter(TypeHelper.GetElementType(source.Type), "x");
                var predicate = Expression.Lambda(p.Equal(match), p);
                var exp = Expression.Call(typeof(Queryable), "Any", new Type[] { p.Type }, source, predicate);

                this.root = exp;

                return this.Visit(exp);
            }
            else
            {
                var projection = this.VisitSequence(source);

                match = this.Visit(match);

                var result = new DbInExpression(match, projection.Select);

                if (isRoot)
                {
                    return this.GetSingletonSequence(result, "SingleOrDefault");
                }

                return result;
            }
        }

        private Expression GetSingletonSequence(Expression expr, string aggregator)
        {
            var p = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(expr.Type), "p");
            var gator = null as LambdaExpression;

            if (aggregator != null)
            {
                gator = Expression.Lambda(Expression.Call(typeof(Enumerable), aggregator, new Type[] { expr.Type }, p), p);
            }

            var alias = this.GetNextAlias();
            var colType = this.language.TypeSystem.GetColumnType(expr.Type);

            var select = new DbSelectExpression(alias, new[] { new DbColumnDeclaration("value", expr, colType) }, null, null);

            return new DbProjectionExpression(select, new DbColumnExpression(expr.Type, colType, alias, "value"), gator);
        }

        private Expression BindInsert(IEntity upd, Expression instance, LambdaExpression selector)
        {
            var entity = this.mapper.Mapping.GetEntity(instance.Type, upd.TableId);

            return this.Visit(this.mapper.GetInsertExpression(entity, instance, selector));
        }

        private Expression BindUpdate(IEntity upd, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            var entity = this.mapper.Mapping.GetEntity(instance.Type, upd.TableId);

            return this.Visit(this.mapper.GetUpdateExpression(entity, instance, updateCheck, resultSelector, null));
        }

        private Expression BindInsertOrUpdate(IEntity upd, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            var entity = this.mapper.Mapping.GetEntity(instance.Type, upd.TableId);

            return this.Visit(this.mapper.GetInsertOrUpdateExpression(entity, instance, updateCheck, resultSelector));
        }

        private Expression BindDelete(IEntity upd, Expression instance, LambdaExpression deleteCheck)
        {
            var entity = this.mapper.Mapping.GetEntity(instance != null ? instance.Type : deleteCheck.Parameters[0].Type, upd.TableId);

            return this.Visit(this.mapper.GetDeleteExpression(entity, instance, deleteCheck));
        }

        private Expression BindBatch(IEntity upd, Expression instances, LambdaExpression operation, Expression batchSize, Expression stream)
        {
            var save = this.batchUpd;
            this.batchUpd = upd;
            var op = this.Visit(operation) as LambdaExpression;
            this.batchUpd = save;
            var items = this.Visit(instances);
            var size = this.Visit(batchSize);
            var str = this.Visit(stream);

            return new DbBatchExpression(items, op, size, str);
        }

        private bool IsQuery(Expression expression)
        {
            var elementType = TypeHelper.GetElementType(expression.Type);

            return elementType != null && typeof(IQueryable<>).MakeGenericType(elementType).IsAssignableFrom(expression.Type);
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (this.IsQuery(c))
            {
                var q = c.Value as IQueryable;

                if (q is IEntity t)
                {
                    var ihme = t as IHaveMappingEntity;
                    var entity = ihme != null ? ihme.Entity : this.mapper.Mapping.GetEntity(t.ElementType, t.TableId);

                    return this.VisitSequence(this.mapper.GetQueryExpression(entity));
                }
                else if (q.Expression.NodeType == ExpressionType.Constant)
                {
                    var entity = this.mapper.Mapping.GetEntity(q.ElementType);

                    return this.VisitSequence(this.mapper.GetQueryExpression(entity));
                }
                else
                {
                    var pev = ExpressionEvaluator.Eval(q.Expression, this.mapper.Mapping.CanBeEvaluatedLocally);

                    return this.Visit(pev);
                }
            }

            return c;
        }

        protected override Expression VisitParameter(ParameterExpression p)
        {
            if (this.map.TryGetValue(p, out Expression e))
            {
                return e;
            }

            return p;
        }

        protected override Expression VisitInvocation(InvocationExpression iv)
        {
            if (iv.Expression is LambdaExpression lambda)
            {
                for (int i = 0, n = lambda.Parameters.Count; i < n; i++)
                {
                    this.map[lambda.Parameters[i]] = iv.Arguments[i];
                }

                return this.Visit(lambda.Body);
            }

            return base.VisitInvocation(iv);
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter && !this.map.ContainsKey((ParameterExpression)m.Expression) && this.IsQuery(m))
            {
                return this.VisitSequence(this.mapper.GetQueryExpression(this.mapper.Mapping.GetEntity(m.Member)));
            }

            var source = this.Visit(m.Expression);

            if (this.language.IsAggregate(m.Member) && IsRemoteQuery(source))
            {
                return this.BindAggregate(m.Expression, m.Member.Name, TypeHelper.GetMemberType(m.Member), null, m == this.root);
            }

            var result = BindMember(source, m.Member);

            if (result is MemberExpression mex && mex.Member == m.Member && mex.Expression == m.Expression)
            {
                return m;
            }

            return result;
        }

        private bool IsRemoteQuery(Expression expression)
        {
            if (expression.NodeType.IsDbExpression())
            {
                return true;
            }

            switch (expression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    {
                        return IsRemoteQuery((expression as MemberExpression).Expression);
                    }
                case ExpressionType.Call:
                    {
                        var mc = expression as MethodCallExpression;

                        if (mc.Object != null)
                        {
                            return IsRemoteQuery(mc.Object);
                        }
                        else if (mc.Arguments.Count > 0)
                        {
                            return IsRemoteQuery(mc.Arguments[0]);
                        }

                        break;
                    }
            }

            return false;
        }

        public static Expression BindMember(Expression source, MemberInfo member)
        {
            switch (source.NodeType)
            {
                case (ExpressionType)DbExpressionType.Entity:
                    {
                        var ex = source as DbEntityExpression;
                        var result = BindMember(ex.Expression, member);

                        if (result is MemberExpression mex && mex.Expression == ex.Expression && mex.Member == member)
                        {
                            return Expression.MakeMemberAccess(source, member);
                        }

                        return result;
                    }
                case ExpressionType.Convert:
                    {
                        var ux = source as UnaryExpression;

                        return BindMember(ux.Operand, member);
                    }
                case ExpressionType.MemberInit:
                    {
                        var min = source as MemberInitExpression;

                        for (int i = 0, n = min.Bindings.Count; i < n; i++)
                        {
                            if (min.Bindings[i] is MemberAssignment assign && MembersMatch(assign.Member, member))
                            {
                                return assign.Expression;
                            }
                        }

                        break;
                    }
                case ExpressionType.New:
                    {
                        var nex = source as NewExpression;

                        if (nex.Members != null)
                        {
                            for (int i = 0, n = nex.Members.Count; i < n; i++)
                            {
                                if (MembersMatch(nex.Members[i], member))
                                {
                                    return nex.Arguments[i];
                                }
                            }
                        }
                        else if (nex.Type.GetTypeInfo().IsGenericType && nex.Type.GetGenericTypeDefinition() == typeof(Grouping<,>))
                        {
                            if (member.Name == "Key")
                            {
                                return nex.Arguments[0];
                            }
                        }

                        break;
                    }
                case (ExpressionType)DbExpressionType.Projection:
                    {
                        var proj = source as DbProjectionExpression;
                        var newProjector = BindMember(proj.Projector, member);
                        var mt = TypeHelper.GetMemberType(member);

                        return new DbProjectionExpression(proj.Select, newProjector, DbAggregator.GetAggregator(mt, typeof(IEnumerable<>).MakeGenericType(mt)));
                    }
                case (ExpressionType)DbExpressionType.OuterJoined:
                    {
                        var oj = source as DbOuterJoinedExpression;
                        var em = BindMember(oj.Expression, member);

                        if (em is DbColumnExpression)
                        {
                            return em;
                        }

                        return new DbOuterJoinedExpression(oj.Test, em);
                    }
                case ExpressionType.Conditional:
                    {
                        var cex = source as ConditionalExpression;

                        return Expression.Condition(cex.Test, BindMember(cex.IfTrue, member), BindMember(cex.IfFalse, member));
                    }
                case ExpressionType.Constant:
                    {
                        var con = source as ConstantExpression;
                        var memberType = TypeHelper.GetMemberType(member);

                        if (con.Value == null)
                        {
                            return Expression.Constant(GetDefault(memberType), memberType);
                        }
                        else
                        {
                            return Expression.Constant(GetValue(con.Value, member), memberType);
                        }
                    }
            }

            return Expression.MakeMemberAccess(source, member);
        }

        private static object GetValue(object instance, MemberInfo member)
        {
            if (member is FieldInfo fi)
            {
                return ReflectionHelper.CreateGetFieldLambda(fi)(instance);
            }

            if (member is PropertyInfo pi)
            {
                return ReflectionHelper.CreateGetPropertyLambda(pi)(instance);
            }

            return null;
        }

        private static object GetDefault(Type type)
        {
            if (type.GetTypeInfo().IsValueType == false || TypeHelper.IsNullableType(type))
            {
                return null;
            }
            else
            {
                return Activator.CreateInstance(type);
            }
        }

        private static bool MembersMatch(MemberInfo a, MemberInfo b)
        {
            if (a.Name == b.Name)
            {
                return true;
            }

            if (a is MethodInfo && b is PropertyInfo)
            {
                return (b as PropertyInfo).GetGetMethod().Name == a.Name;
            }
            else if (a is PropertyInfo && b is MethodInfo)
            {
                return (a as PropertyInfo).GetGetMethod().Name == b.Name;
            }

            return false;
        }
    }
}