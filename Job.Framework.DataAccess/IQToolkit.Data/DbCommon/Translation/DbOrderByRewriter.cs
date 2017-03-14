using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Common
{
    internal class DbOrderByRewriter : DbExpressionVisitor
    {
        private bool isOuterMostSelect;
        private readonly QueryLanguage language;
        private IList<DbOrderExpression> gatheredOrderings;

        private DbOrderByRewriter(QueryLanguage language)
        {
            this.language = language;
            this.isOuterMostSelect = true;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbOrderByRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            bool saveIsOuterMostSelect = this.isOuterMostSelect;

            try
            {
                this.isOuterMostSelect = false;

                select = base.VisitSelect(select) as DbSelectExpression;

                var hasOrderBy = select.OrderBy != null && select.OrderBy.Count > 0;
                var hasGroupBy = select.GroupBy != null && select.GroupBy.Count > 0;
                var canHaveOrderBy = saveIsOuterMostSelect || select.Take != null || select.Skip != null;
                var canReceiveOrderings = canHaveOrderBy && !hasGroupBy && !select.IsDistinct && !DbAggregateChecker.HasAggregates(select);

                if (hasOrderBy)
                {
                    this.PrependOrderings(select.OrderBy);
                }

                if (select.IsReverse)
                {
                    this.ReverseOrderings();
                }

                var orderings = null as IEnumerable<DbOrderExpression>;

                if (canReceiveOrderings)
                {
                    orderings = this.gatheredOrderings;
                }
                else if (canHaveOrderBy)
                {
                    orderings = select.OrderBy;
                }

                var canPassOnOrderings = !saveIsOuterMostSelect && !hasGroupBy && !select.IsDistinct;
                var columns = select.Columns;

                if (this.gatheredOrderings != null)
                {
                    if (canPassOnOrderings)
                    {
                        var producedAliases = DbDeclaredAliasGatherer.Gather(select.From);
                        var project = this.RebindOrderings(this.gatheredOrderings, select.Alias, producedAliases, select.Columns);

                        this.gatheredOrderings = null;
                        this.PrependOrderings(project.Orderings);

                        columns = project.Columns;
                    }
                    else
                    {
                        this.gatheredOrderings = null;
                    }
                }

                if (orderings != select.OrderBy || columns != select.Columns || select.IsReverse)
                {
                    select = new DbSelectExpression(select.Alias, columns, select.From, select.Where, orderings, select.GroupBy, select.IsDistinct, select.Skip, select.Take, false);
                }

                return select;
            }
            finally
            {
                this.isOuterMostSelect = saveIsOuterMostSelect;
            }
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            var saveOrderings = this.gatheredOrderings;
            this.gatheredOrderings = null;
            var result = base.VisitSubquery(subquery);
            this.gatheredOrderings = saveOrderings;

            return result;
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            var left = this.VisitSource(join.Left);
            var leftOrders = this.gatheredOrderings;

            this.gatheredOrderings = null;

            var right = this.VisitSource(join.Right);
            this.PrependOrderings(leftOrders);
            var condition = this.Visit(join.Condition);

            if (left != join.Left || right != join.Right || condition != join.Condition)
            {
                return new DbJoinExpression(join.JoinType, left, right, condition);
            }

            return join;
        }

        protected void PrependOrderings(IList<DbOrderExpression> newOrderings)
        {
            if (newOrderings != null)
            {
                if (this.gatheredOrderings == null)
                {
                    this.gatheredOrderings = new List<DbOrderExpression>();
                }

                for (var i = newOrderings.Count - 1; i >= 0; i--)
                {
                    this.gatheredOrderings.Insert(0, newOrderings[i]);
                }

                var unique = new HashSet<string>();

                for (var i = 0; i < this.gatheredOrderings.Count;)
                {
                    if (this.gatheredOrderings[i].Expression is DbColumnExpression column)
                    {
                        var hash = column.Alias + ":" + column.Name;

                        if (unique.Contains(hash))
                        {
                            this.gatheredOrderings.RemoveAt(i);

                            continue;
                        }
                        else
                        {
                            unique.Add(hash);
                        }
                    }
                    i++;
                }
            }
        }

        protected void ReverseOrderings()
        {
            if (this.gatheredOrderings != null)
            {
                for (int i = 0, n = this.gatheredOrderings.Count; i < n; i++)
                {
                    var ord = this.gatheredOrderings[i];

                    this.gatheredOrderings[i] = new DbOrderExpression
                    (
                        ord.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                        ord.Expression
                    );
                }
            }
        }

        protected class BindResult
        {
            public ReadOnlyCollection<DbColumnDeclaration> Columns { get; }

            public ReadOnlyCollection<DbOrderExpression> Orderings { get; }

            public BindResult(IEnumerable<DbColumnDeclaration> columns, IEnumerable<DbOrderExpression> orderings)
            {
                this.Columns = columns as ReadOnlyCollection<DbColumnDeclaration>;

                if (this.Columns == null)
                {
                    this.Columns = new List<DbColumnDeclaration>(columns).AsReadOnly();
                }

                this.Orderings = orderings as ReadOnlyCollection<DbOrderExpression>;

                if (this.Orderings == null)
                {
                    this.Orderings = new List<DbOrderExpression>(orderings).AsReadOnly();
                }
            }
        }

        protected virtual BindResult RebindOrderings(IEnumerable<DbOrderExpression> orderings, TableAlias alias, HashSet<TableAlias> existingAliases, IEnumerable<DbColumnDeclaration> existingColumns)
        {
            var newColumns = null as List<DbColumnDeclaration>;
            var newOrderings = new List<DbOrderExpression>();

            foreach (var ordering in orderings)
            {
                var expr = ordering.Expression;
                var column = expr as DbColumnExpression;

                if (column == null || (existingAliases != null && existingAliases.Contains(column.Alias)))
                {
                    var iOrdinal = 0;

                    foreach (var decl in existingColumns)
                    {
                        var declColumn = decl.Expression as DbColumnExpression;

                        if (decl.Expression == ordering.Expression || (column != null && declColumn != null && column.Alias == declColumn.Alias && column.Name == declColumn.Name))
                        {
                            expr = new DbColumnExpression(column.Type, column.QueryType, alias, decl.Name);

                            break;
                        }

                        iOrdinal++;
                    }

                    if (expr == ordering.Expression)
                    {
                        if (newColumns == null)
                        {
                            newColumns = new List<DbColumnDeclaration>(existingColumns);
                            existingColumns = newColumns;
                        }

                        var colName = column != null ? column.Name : "c" + iOrdinal;

                        colName = newColumns.GetAvailableColumnName(colName);

                        var colType = this.language.TypeSystem.GetColumnType(expr.Type);

                        newColumns.Add(new DbColumnDeclaration(colName, ordering.Expression, colType));

                        expr = new DbColumnExpression(expr.Type, colType, alias, colName);
                    }

                    newOrderings.Add(new DbOrderExpression(ordering.OrderType, expr));
                }
            }

            return new BindResult(existingColumns, newOrderings);
        }
    }
}