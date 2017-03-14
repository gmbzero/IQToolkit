using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbUnusedColumnRemover : DbExpressionVisitor
    {
        private bool retainAllColumns;
        private readonly Dictionary<TableAlias, HashSet<string>> allColumnsUsed;

        private DbUnusedColumnRemover()
        {
            this.allColumnsUsed = new Dictionary<TableAlias, HashSet<string>>();
        }

        public static Expression Remove(Expression expression)
        {
            return new DbUnusedColumnRemover().Visit(expression);
        }

        private void MarkColumnAsUsed(TableAlias alias, string name)
        {
            if (this.allColumnsUsed.TryGetValue(alias, out HashSet<string> columns) == false)
            {
                columns = new HashSet<string>();

                this.allColumnsUsed.Add(alias, columns);
            }

            columns.Add(name);
        }

        private bool IsColumnUsed(TableAlias alias, string name)
        {
            if (this.allColumnsUsed.TryGetValue(alias, out HashSet<string> columnsUsed))
            {
                if (columnsUsed != null)
                {
                    return columnsUsed.Contains(name);
                }
            }

            return false;
        }

        private void ClearColumnsUsed(TableAlias alias)
        {
            this.allColumnsUsed[alias] = new HashSet<string>();
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            MarkColumnAsUsed(column.Alias, column.Name); return column;
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            if ((subquery.NodeType == (ExpressionType)DbExpressionType.Scalar || subquery.NodeType == (ExpressionType)DbExpressionType.In) && subquery.Select != null)
            {
                MarkColumnAsUsed(subquery.Select.Alias, subquery.Select.Columns[0].Name);
            }

            return base.VisitSubquery(subquery);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var columns = select.Columns;
            var wasRetained = this.retainAllColumns;

            this.retainAllColumns = false;

            var alternate = null as List<DbColumnDeclaration>;

            for (int i = 0, n = select.Columns.Count; i < n; i++)
            {
                var decl = select.Columns[i];

                if (wasRetained || select.IsDistinct || IsColumnUsed(select.Alias, decl.Name))
                {
                    var expr = this.Visit(decl.Expression);

                    if (expr != decl.Expression)
                    {
                        decl = new DbColumnDeclaration(decl.Name, expr, decl.QueryType);
                    }
                }
                else
                {
                    decl = null;
                }

                if (decl != select.Columns[i] && alternate == null)
                {
                    alternate = new List<DbColumnDeclaration>();

                    for (var j = 0; j < i; j++)
                    {
                        alternate.Add(select.Columns[j]);
                    }
                }

                if (decl != null && alternate != null)
                {
                    alternate.Add(decl);
                }
            }

            if (alternate != null)
            {
                columns = alternate.AsReadOnly();
            }

            var take = this.Visit(select.Take);
            var skip = this.Visit(select.Skip);
            var groupbys = this.VisitExpressionList(select.GroupBy);
            var orderbys = this.VisitOrderBy(select.OrderBy);
            var where = this.Visit(select.Where);
            var from = this.Visit(select.From);

            ClearColumnsUsed(select.Alias);

            if (columns != select.Columns || take != select.Take || skip != select.Skip || orderbys != select.OrderBy || groupbys != select.GroupBy || where != select.Where || from != select.From)
            {
                select = new DbSelectExpression(select.Alias, columns, from, where, orderbys, groupbys, select.IsDistinct, skip, take, select.IsReverse);
            }

            this.retainAllColumns = wasRetained;

            return select;
        }

        protected override Expression VisitAggregate(DbAggregateExpression aggregate)
        {
            if (aggregate.AggregateName == "Count" && aggregate.Argument == null)
            {
                this.retainAllColumns = true;
            }

            return base.VisitAggregate(aggregate);
        }

        protected override Expression VisitProjection(DbProjectionExpression projection)
        {
            var projector = this.Visit(projection.Projector);
            var select = this.Visit(projection.Select) as DbSelectExpression;

            return this.UpdateProjection(projection, select, projector, projection.Aggregator);
        }

        protected override Expression VisitClientJoin(DbClientJoinExpression join)
        {
            var innerKey = this.VisitExpressionList(join.InnerKey);
            var outerKey = this.VisitExpressionList(join.OuterKey);
            var projection = this.Visit(join.Projection) as DbProjectionExpression;

            if (projection != join.Projection || innerKey != join.InnerKey || outerKey != join.OuterKey)
            {
                return new DbClientJoinExpression(projection, outerKey, innerKey);
            }

            return join;
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            if (join.JoinType == JoinType.SingletonLeftOuter)
            {
                var right = this.Visit(join.Right);

                if (right is DbAliasedExpression ax && !this.allColumnsUsed.ContainsKey(ax.Alias))
                {
                    return this.Visit(join.Left);
                }

                var cond = this.Visit(join.Condition);
                var left = this.Visit(join.Left);

                right = this.Visit(join.Right);

                return this.UpdateJoin(join, join.JoinType, left, right, cond);
            }
            else
            {
                var condition = this.Visit(join.Condition);
                var right = this.VisitSource(join.Right);
                var left = this.VisitSource(join.Left);

                return this.UpdateJoin(join, join.JoinType, left, right, condition);
            }
        }
    }
}