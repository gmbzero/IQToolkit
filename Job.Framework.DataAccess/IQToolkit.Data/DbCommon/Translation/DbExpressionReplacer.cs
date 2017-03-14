using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbExpressionReplacer : DbExpressionVisitor
    {
        private readonly Expression searchFor;
        private readonly Expression replaceWith;

        private DbExpressionReplacer(Expression searchFor, Expression replaceWith)
        {
            this.searchFor = searchFor;
            this.replaceWith = replaceWith;
        }

        public static Expression Replace(Expression expression, Expression searchFor, Expression replaceWith)
        {
            return new DbExpressionReplacer(searchFor, replaceWith).Visit(expression);
        }

        public static Expression ReplaceAll(Expression expression, Expression[] searchFor, Expression[] replaceWith)
        {
            for (int i = 0, n = searchFor.Length; i < n; i++)
            {
                expression = Replace(expression, searchFor[i], replaceWith[i]);
            }

            return expression;
        }

        protected override Expression Visit(Expression exp)
        {
            if (exp == this.searchFor)
            {
                return this.replaceWith;
            }

            return base.Visit(exp);
        }
    }
}