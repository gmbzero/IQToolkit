using System;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit
{
    internal class ExpressionSubFinder : ExpressionVisitor
    {
        private Expression root;
        private readonly Type type;

        private ExpressionSubFinder(Type type)
        {
            this.type = type;
        }

        public static Expression Find(Expression expression, Type type)
        {
            var finder = new ExpressionSubFinder(type);

            if (finder != null)
            {
                finder.Visit(expression);
            }

            return finder.root;
        }

        protected override Expression Visit(Expression exp)
        {
            var result = base.Visit(exp);

            if (result != null && this.root == null)
            {
                if (this.type.IsAssignableFrom(result.Type))
                {
                    this.root = result;
                }
            }

            return result;
        }
    }
}