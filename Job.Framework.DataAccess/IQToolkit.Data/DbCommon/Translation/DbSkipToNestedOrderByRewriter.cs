using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbSkipToNestedOrderByRewriter : DbExpressionVisitor
    {
        private readonly QueryLanguage language;

        private DbSkipToNestedOrderByRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbSkipToNestedOrderByRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            select = base.VisitSelect(select) as DbSelectExpression;

            if (select.Skip != null && select.Take != null && select.OrderBy.Count > 0)
            {
                var skip = select.Skip;
                var take = select.Take;
                var skipPlusTake = ExpressionEvaluator.Eval(Expression.Add(skip, take));

                select = select.SetTake(skipPlusTake).SetSkip(null);
                select = select.AddRedundantSelect(this.language, new TableAlias());
                select = select.SetTake(take);
                select = DbOrderByRewriter.Rewrite(this.language, select) as DbSelectExpression;

                var inverted = select.OrderBy.Select(ob => new DbOrderExpression
                (
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                 ));

                select = select.SetOrderBy(inverted);
                select = select.AddRedundantSelect(this.language, new TableAlias());
                select = select.SetTake(Expression.Constant(0));
                select = DbOrderByRewriter.Rewrite(this.language, select) as DbSelectExpression;

                var reverted = select.OrderBy.Select(ob => new DbOrderExpression
                (
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                ));

                select = select.SetOrderBy(reverted);
                select = select.SetTake(null);
            }

            return select;
        }
    }
}