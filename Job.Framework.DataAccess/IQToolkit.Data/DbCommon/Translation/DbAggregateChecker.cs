using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbAggregateChecker : DbExpressionVisitor
    {
        private bool hasAggregate = false;

        public static bool HasAggregates(DbSelectExpression expression)
        {
            var checker = new DbAggregateChecker();

            if (checker != null)
            {
                checker.Visit(expression);
            }

            return checker.hasAggregate;
        }

        protected override Expression VisitAggregate(DbAggregateExpression aggregate)
        {
            this.hasAggregate = true; return aggregate;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            this.Visit(select.Where);
            this.VisitOrderBy(select.OrderBy);
            this.VisitColumnDeclarations(select.Columns);

            return select;
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            return subquery;
        }
    }
}