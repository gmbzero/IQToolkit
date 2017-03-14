using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbRedundantSubqueryRemover : DbExpressionVisitor
    {
        public static Expression Remove(Expression expression)
        {
            expression = new DbRedundantSubqueryRemover().Visit(expression);
            expression = SubqueryMerger.Merge(expression);

            return expression;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            select = base.VisitSelect(select) as DbSelectExpression;

            var redundant = RedundantSubqueryGatherer.Gather(select.From);

            if (redundant != null)
            {
                select = DbSubqueryRemover.Remove(select, redundant);
            }

            return select;
        }

        protected override Expression VisitProjection(DbProjectionExpression proj)
        {
            proj = base.VisitProjection(proj) as DbProjectionExpression;

            if (proj.Select.From is DbSelectExpression)
            {
                var redundant = RedundantSubqueryGatherer.Gather(proj.Select);

                if (redundant != null)
                {
                    proj = DbSubqueryRemover.Remove(proj, redundant);
                }
            }

            return proj;
        }

        public static bool IsSimpleProjection(DbSelectExpression select)
        {
            foreach (var decl in select.Columns)
            {
                var col = decl.Expression as DbColumnExpression;

                if (col == null || decl.Name != col.Name)
                {
                    return false;
                }
            }

            return true;
        }

        public static bool IsNameMapProjection(DbSelectExpression select)
        {
            if (select.From is DbTableExpression)
            {
                return false;
            }

            var fromSelect = select.From as DbSelectExpression;

            if (fromSelect == null || select.Columns.Count != fromSelect.Columns.Count)
            {
                return false;
            }

            var fromColumns = fromSelect.Columns;

            for (int i = 0, n = select.Columns.Count; i < n; i++)
            {
                var col = select.Columns[i].Expression as DbColumnExpression;

                if (col == null || !(col.Name == fromColumns[i].Name))
                {
                    return false;
                }
            }

            return true;
        }

        public static bool IsInitialProjection(DbSelectExpression select)
        {
            return select.From is DbTableExpression;
        }

        private class RedundantSubqueryGatherer : DbExpressionVisitor
        {
            private List<DbSelectExpression> redundant;

            public static List<DbSelectExpression> Gather(Expression source)
            {
                var gatherer = new RedundantSubqueryGatherer();

                if (gatherer != null)
                {
                    gatherer.Visit(source);
                }

                return gatherer.redundant;
            }

            private static bool IsRedudantSubquery(DbSelectExpression select)
            {
                return
                (
                    (IsSimpleProjection(select) || IsNameMapProjection(select))
                    && !select.IsDistinct
                    && !select.IsReverse
                    && select.Take == null
                    && select.Skip == null
                    && select.Where == null
                    && (select.OrderBy == null || select.OrderBy.Count == 0)
                    && (select.GroupBy == null || select.GroupBy.Count == 0)
                );
            }

            protected override Expression VisitSelect(DbSelectExpression select)
            {
                if (IsRedudantSubquery(select))
                {
                    if (this.redundant == null)
                    {
                        this.redundant = new List<DbSelectExpression>();
                    }

                    this.redundant.Add(select);
                }

                return select;
            }

            protected override Expression VisitSubquery(SubqueryExpression subquery)
            {
                return subquery;
            }
        }

        private class SubqueryMerger : DbExpressionVisitor
        {
            private bool isTopLevel = true;

            public static Expression Merge(Expression expression)
            {
                return new SubqueryMerger().Visit(expression);
            }

            protected override Expression VisitSelect(DbSelectExpression select)
            {
                var wasTopLevel = isTopLevel;

                isTopLevel = false;

                select = base.VisitSelect(select) as DbSelectExpression;

                while (CanMergeWithFrom(select, wasTopLevel))
                {
                    var fromSelect = GetLeftMostSelect(select.From);

                    select = DbSubqueryRemover.Remove(select, fromSelect);

                    var where = select.Where;

                    if (fromSelect.Where != null)
                    {
                        if (where != null)
                        {
                            where = fromSelect.Where.And(where);
                        }
                        else
                        {
                            where = fromSelect.Where;
                        }
                    }

                    var orderBy = select.OrderBy != null && select.OrderBy.Count > 0 ? select.OrderBy : fromSelect.OrderBy;
                    var groupBy = select.GroupBy != null && select.GroupBy.Count > 0 ? select.GroupBy : fromSelect.GroupBy;
                    var skip = select.Skip ?? fromSelect.Skip;
                    var take = select.Take ?? fromSelect.Take;

                    var isDistinct = select.IsDistinct | fromSelect.IsDistinct;

                    if (where != select.Where || orderBy != select.OrderBy || groupBy != select.GroupBy || isDistinct != select.IsDistinct || skip != select.Skip || take != select.Take)
                    {
                        select = new DbSelectExpression(select.Alias, select.Columns, select.From, where, orderBy, groupBy, isDistinct, skip, take, select.IsReverse);
                    }
                }

                return select;
            }

            private static DbSelectExpression GetLeftMostSelect(Expression source)
            {
                if (source is DbSelectExpression select)
                {
                    return select;
                }

                if (source is DbJoinExpression join)
                {
                    return GetLeftMostSelect(join.Left);
                }

                return null;
            }

            private static bool IsColumnProjection(DbSelectExpression select)
            {
                for (int i = 0, n = select.Columns.Count; i < n; i++)
                {
                    var cd = select.Columns[i];

                    if (cd.Expression.NodeType != (ExpressionType)DbExpressionType.Column && cd.Expression.NodeType != ExpressionType.Constant)
                    {
                        return false;
                    }
                }

                return true;
            }

            private static bool CanMergeWithFrom(DbSelectExpression select, bool isTopLevel)
            {
                var fromSelect = GetLeftMostSelect(select.From);

                if (fromSelect == null)
                {
                    return false;
                }

                if (IsColumnProjection(fromSelect) == false)
                {
                    return false;
                }

                var selHasNameMapProjection = IsNameMapProjection(select);
                var selHasOrderBy = select.OrderBy != null && select.OrderBy.Count > 0;
                var selHasGroupBy = select.GroupBy != null && select.GroupBy.Count > 0;
                var selHasAggregates = DbAggregateChecker.HasAggregates(select);
                var selHasJoin = select.From is DbJoinExpression;
                var frmHasOrderBy = fromSelect.OrderBy != null && fromSelect.OrderBy.Count > 0;
                var frmHasGroupBy = fromSelect.GroupBy != null && fromSelect.GroupBy.Count > 0;
                var frmHasAggregates = DbAggregateChecker.HasAggregates(fromSelect);

                if (selHasOrderBy && frmHasOrderBy) return false;

                if (selHasGroupBy && frmHasGroupBy) return false;

                if (select.IsReverse || fromSelect.IsReverse) return false;

                if (frmHasOrderBy && (selHasGroupBy || selHasAggregates || select.IsDistinct)) return false;

                if (frmHasGroupBy) return false;

                if (fromSelect.Take != null && (select.Take != null || select.Skip != null || select.IsDistinct || selHasAggregates || selHasGroupBy || selHasJoin)) return false;

                if (fromSelect.Skip != null && (select.Skip != null || select.IsDistinct || selHasAggregates || selHasGroupBy || selHasJoin)) return false;

                if (fromSelect.IsDistinct && (select.Take != null || select.Skip != null || !selHasNameMapProjection || selHasGroupBy || selHasAggregates || (selHasOrderBy && !isTopLevel) || selHasJoin)) return false;

                if (frmHasAggregates && (select.Take != null || select.Skip != null || select.IsDistinct || selHasAggregates || selHasGroupBy || selHasJoin)) return false;

                return true;
            }
        }
    }
}