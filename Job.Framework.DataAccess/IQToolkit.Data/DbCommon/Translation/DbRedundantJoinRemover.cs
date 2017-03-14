using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbRedundantJoinRemover : DbExpressionVisitor
    {
        private readonly Dictionary<TableAlias, TableAlias> map;

        private DbRedundantJoinRemover()
        {
            this.map = new Dictionary<TableAlias, TableAlias>();
        }

        public static Expression Remove(Expression expression)
        {
            return new DbRedundantJoinRemover().Visit(expression);
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            var result = base.VisitJoin(join);

            join = result as DbJoinExpression;

            if (join != null)
            {
                if (join.Right is DbAliasedExpression right)
                {
                    if (this.FindSimilarRight(join.Left as DbJoinExpression, join) is DbAliasedExpression similarRight)
                    {
                        this.map.Add(right.Alias, similarRight.Alias);

                        return join.Left;
                    }
                }
            }

            return result;
        }

        private Expression FindSimilarRight(DbJoinExpression join, DbJoinExpression compareTo)
        {
            if (join == null)
            {
                return null;
            }

            if (join.JoinType == compareTo.JoinType)
            {
                if (join.Right.NodeType == compareTo.Right.NodeType && DbExpressionComparer.AreEqual(join.Right, compareTo.Right))
                {
                    if (join.Condition == compareTo.Condition)
                    {
                        return join.Right;
                    }

                    var scope = new ScopedDictionary<TableAlias, TableAlias>(null);

                    if (scope != null)
                    {
                        scope.Add((join.Right as DbAliasedExpression).Alias, (compareTo.Right as DbAliasedExpression).Alias);
                    }

                    if (DbExpressionComparer.AreEqual(null, scope, join.Condition, compareTo.Condition))
                    {
                        return join.Right;
                    }
                }
            }

            var result = FindSimilarRight(join.Left as DbJoinExpression, compareTo);

            if (result == null)
            {
                result = FindSimilarRight(join.Right as DbJoinExpression, compareTo);
            }

            return result;
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.map.TryGetValue(column.Alias, out TableAlias mapped))
            {
                return new DbColumnExpression(column.Type, column.QueryType, mapped, column.Name);
            }

            return column;
        }
    }
}