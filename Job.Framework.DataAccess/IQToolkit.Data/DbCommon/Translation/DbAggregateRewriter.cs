using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbAggregateRewriter : DbExpressionVisitor
    {
        private readonly QueryLanguage language;
        private readonly ILookup<TableAlias, DbAggregateSubqueryExpression> lookup;
        private readonly Dictionary<DbAggregateSubqueryExpression, Expression> map;

        private DbAggregateRewriter(QueryLanguage language, Expression expr)
        {
            this.language = language;
            this.map = new Dictionary<DbAggregateSubqueryExpression, Expression>();
            this.lookup = AggregateGatherer.Gather(expr).ToLookup(a => a.GroupByAlias);
        }

        public static Expression Rewrite(QueryLanguage language, Expression expr)
        {
            return new DbAggregateRewriter(language, expr).Visit(expr);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            select = base.VisitSelect(select) as DbSelectExpression;

            if (lookup.Contains(select.Alias))
            {
                var aggColumns = new List<DbColumnDeclaration>(select.Columns);

                foreach (var ae in lookup[select.Alias])
                {
                    var name = "agg" + aggColumns.Count;
                    var colType = this.language.TypeSystem.GetColumnType(ae.Type);
                    var cd = new DbColumnDeclaration(name, ae.AggregateInGroupSelect, colType);

                    this.map.Add(ae, new DbColumnExpression(ae.Type, colType, ae.GroupByAlias, name));

                    aggColumns.Add(cd);
                }

                return new DbSelectExpression(select.Alias, aggColumns, select.From, select.Where, select.OrderBy, select.GroupBy, select.IsDistinct, select.Skip, select.Take, select.IsReverse);
            }

            return select;
        }

        protected override Expression VisitAggregateSubquery(DbAggregateSubqueryExpression aggregate)
        {
            if (this.map.TryGetValue(aggregate, out Expression mapped))
            {
                return mapped;
            }

            return this.Visit(aggregate.AggregateAsSubquery);
        }

        private class AggregateGatherer : DbExpressionVisitor
        {
            private readonly List<DbAggregateSubqueryExpression> aggregates;

            private AggregateGatherer()
            {
                this.aggregates = new List<DbAggregateSubqueryExpression>();
            }

            internal static List<DbAggregateSubqueryExpression> Gather(Expression expression)
            {
                var gatherer = new AggregateGatherer();

                if (gatherer != null)
                {
                    gatherer.Visit(expression);
                }

                return gatherer.aggregates;
            }

            protected override Expression VisitAggregateSubquery(DbAggregateSubqueryExpression aggregate)
            {
                this.aggregates.Add(aggregate);

                return base.VisitAggregateSubquery(aggregate);
            }
        }
    }
}