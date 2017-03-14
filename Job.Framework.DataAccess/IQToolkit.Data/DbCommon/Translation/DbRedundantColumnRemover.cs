using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbRedundantColumnRemover : DbExpressionVisitor
    {
        private readonly Dictionary<DbColumnExpression, DbColumnExpression> map;

        private DbRedundantColumnRemover()
        {
            this.map = new Dictionary<DbColumnExpression, DbColumnExpression>();
        }

        public static Expression Remove(Expression expression)
        {
            return new DbRedundantColumnRemover().Visit(expression);
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.map.TryGetValue(column, out DbColumnExpression mapped))
            {
                return mapped;
            }

            return column;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            select = base.VisitSelect(select) as DbSelectExpression;

            var cols = select.Columns.OrderBy(c => c.Name).ToList();
            var removed = new BitArray(select.Columns.Count);
            var anyRemoved = false;

            for (int i = 0, n = cols.Count; i < n - 1; i++)
            {
                var ci = cols[i];
                var cix = ci.Expression as DbColumnExpression;
                var qt = cix != null ? cix.QueryType : ci.QueryType;
                var cxi = new DbColumnExpression(ci.Expression.Type, qt, select.Alias, ci.Name);

                for (int j = i + 1; j < n; j++)
                {
                    if (!removed.Get(j))
                    {
                        var cj = cols[j];

                        if (SameExpression(ci.Expression, cj.Expression))
                        {
                            var cxj = new DbColumnExpression(cj.Expression.Type, qt, select.Alias, cj.Name);

                            this.map.Add(cxj, cxi);
                            removed.Set(j, true);
                            anyRemoved = true;
                        }
                    }
                }
            }

            if (anyRemoved)
            {
                var newDecls = new List<DbColumnDeclaration>();

                for (int i = 0, n = cols.Count; i < n; i++)
                {
                    if (!removed.Get(i))
                    {
                        newDecls.Add(cols[i]);
                    }
                }

                select = select.SetColumns(newDecls);
            }

            return select;
        }

        private bool SameExpression(Expression a, Expression b)
        {
            if (a == b)
            {
                return true;
            }

            var ca = a as DbColumnExpression;
            var cb = b as DbColumnExpression;

            return (ca != null && cb != null && ca.Alias == cb.Alias && ca.Name == cb.Name);
        }
    }
}