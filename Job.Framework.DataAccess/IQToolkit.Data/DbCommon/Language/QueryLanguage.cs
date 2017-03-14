using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit.Data.Common
{
    internal abstract class QueryLanguage
    {
        public abstract QueryTypeSystem TypeSystem { get; }

        public abstract Expression GetGeneratedIdExpression(MemberInfo member);

        public virtual string Quote(string name)
        {
            return name;
        }

        public virtual bool AllowsMultipleCommands
        {
            get { return false; }
        }

        public virtual bool AllowSubqueryInSelectWithoutFrom
        {
            get { return false; }
        }

        public virtual bool AllowDistinctInAggregates
        {
            get { return false; }
        }

        public virtual Expression GetRowsAffectedExpression(Expression command)
        {
            return new DbFunctionExpression(typeof(int), "@@ROWCOUNT", null);
        }

        public virtual bool IsRowsAffectedExpressions(Expression expression)
        {
            return expression is DbFunctionExpression fex && fex.Name == "@@ROWCOUNT";
        }

        internal virtual Expression GetOuterJoinTest(DbSelectExpression select)
        {
            var aliases = DbDeclaredAliasGatherer.Gather(select.From);
            var joinColumns = JoinColumnGatherer.Gather(aliases, select).ToList();

            if (joinColumns.Count > 0)
            {
                foreach (var jc in joinColumns)
                {
                    foreach (var col in select.Columns)
                    {
                        if (jc.Equals(col.Expression))
                        {
                            return jc;
                        }
                    }
                }

                return joinColumns[0];
            }

            return Expression.Constant(1, typeof(int?));
        }

        internal virtual DbProjectionExpression AddOuterJoinTest(DbProjectionExpression proj)
        {
            var test = this.GetOuterJoinTest(proj.Select);
            var select = proj.Select;
            var testCol = null as DbColumnExpression;

            foreach (var col in select.Columns)
            {
                if (test.Equals(col.Expression))
                {
                    testCol = new DbColumnExpression(test.Type, TypeSystem.GetColumnType(test.Type), select.Alias, col.Name);

                    break;
                }
            }

            if (testCol == null)
            {
                testCol = test as DbColumnExpression;

                var colName = testCol != null ? testCol.Name : "Test";

                if (colName != null)
                {
                    colName = proj.Select.Columns.GetAvailableColumnName(colName);
                }

                var colType = this.TypeSystem.GetColumnType(test.Type);

                if (colType != null)
                {
                    select = select.AddColumn(new DbColumnDeclaration(colName, test, colType));

                    testCol = new DbColumnExpression(test.Type, colType, select.Alias, colName);
                }
            }

            return new DbProjectionExpression
            (
                select,
                new DbOuterJoinedExpression(testCol, proj.Projector),
                proj.Aggregator
            );
        }

        private class JoinColumnGatherer
        {
            private readonly HashSet<TableAlias> aliases;
            private HashSet<DbColumnExpression> columns = new HashSet<DbColumnExpression>();

            private JoinColumnGatherer(HashSet<TableAlias> aliases)
            {
                this.aliases = aliases;
            }

            public static HashSet<DbColumnExpression> Gather(HashSet<TableAlias> aliases, DbSelectExpression select)
            {
                var gatherer = new JoinColumnGatherer(aliases);

                if (gatherer != null)
                {
                    gatherer.Gather(select.Where);
                }

                return gatherer.columns;
            }

            private void Gather(Expression expression)
            {
                if (expression is BinaryExpression b)
                {
                    switch (b.NodeType)
                    {
                        case ExpressionType.Equal:
                        case ExpressionType.NotEqual:
                            {
                                if (IsExternalColumn(b.Left) && GetColumn(b.Right) != null)
                                {
                                    this.columns.Add(GetColumn(b.Right));
                                }
                                else if (IsExternalColumn(b.Right) && GetColumn(b.Left) != null)
                                {
                                    this.columns.Add(GetColumn(b.Left));
                                }

                                break;
                            }
                        case ExpressionType.And:
                        case ExpressionType.AndAlso:
                            {
                                if (b.Type == typeof(bool) || b.Type == typeof(bool?))
                                {
                                    this.Gather(b.Left);
                                    this.Gather(b.Right);
                                }

                                break;
                            }
                    }
                }
            }

            private DbColumnExpression GetColumn(Expression exp)
            {
                while (exp.NodeType == ExpressionType.Convert)
                {
                    exp = (exp as UnaryExpression).Operand;
                }

                return exp as DbColumnExpression;
            }

            private bool IsExternalColumn(Expression exp)
            {
                var col = GetColumn(exp);

                if (col != null && !this.aliases.Contains(col.Alias))
                {
                    return true;
                }

                return false;
            }
        }

        public virtual bool IsScalar(Type type)
        {
            if (TypeHelper.IsNullableType(type))
            {
                return false;
            }
            else
            {
                type = TypeHelper.GetNonNullableType(type);
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                    {
                        return false;
                    }
                case TypeCode.Object:
                    {
                        return
                        (
                            type == typeof(DateTimeOffset) ||
                            type == typeof(TimeSpan) ||
                            type == typeof(Guid) ||
                            type == typeof(byte[])
                        );
                    }
                default:
                    {
                        return true;
                    }
            }
        }

        public virtual bool IsAggregate(MemberInfo member)
        {
            if (member is MethodInfo method)
            {
                if (method.DeclaringType == typeof(Queryable) || method.DeclaringType == typeof(Enumerable))
                {
                    switch (method.Name)
                    {
                        case "Count":
                        case "LongCount":
                        case "Sum":
                        case "Min":
                        case "Max":
                        case "Average":
                            {
                                return true;
                            }
                    }
                }
            }

            if (member is PropertyInfo property && property.Name == "Count" && typeof(IEnumerable).IsAssignableFrom(property.DeclaringType))
            {
                return true;
            }

            return false;
        }

        public virtual bool AggregateArgumentIsPredicate(string aggregateName)
        {
            return aggregateName == "Count" || aggregateName == "LongCount";
        }

        public virtual bool CanBeColumn(Expression expression)
        {
            return this.MustBeColumn(expression);
        }

        public virtual bool MustBeColumn(Expression expression)
        {
            switch (expression.NodeType)
            {
                case (ExpressionType)DbExpressionType.Column:
                case (ExpressionType)DbExpressionType.Scalar:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.AggregateSubquery:
                case (ExpressionType)DbExpressionType.Aggregate:
                    {
                        return true;
                    }
                default:
                    {
                        return false;
                    }
            }
        }

        public virtual QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new QueryLinguist(this, translator);
        }
    }
}