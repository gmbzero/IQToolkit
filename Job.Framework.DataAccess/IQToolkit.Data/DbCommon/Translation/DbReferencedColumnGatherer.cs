using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbReferencedColumnGatherer : DbExpressionVisitor
    {
        private bool first = true;
        private readonly HashSet<DbColumnExpression> columns;

        private DbReferencedColumnGatherer()
        {
            this.columns = new HashSet<DbColumnExpression>();
        }

        public static HashSet<DbColumnExpression> Gather(Expression expression)
        {
            var visitor = new DbReferencedColumnGatherer();

            if (visitor != null)
            {
                visitor.Visit(expression);
            }

            return visitor.columns;
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            this.columns.Add(column); return column;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            if (first)
            {
                first = false;

                return base.VisitSelect(select);
            }

            return select;
        }
    }
}