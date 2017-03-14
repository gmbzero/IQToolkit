using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace IQToolkit
{
    internal abstract class QueryProvider : IQueryProvider, IQueryText
    {
        protected QueryProvider()
        {

        }

        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            var elementType = TypeHelper.GetElementType(expression.Type);

            try
            {
                return Activator.CreateInstance(typeof(Query<TElement>), new object[] { this, expression }) as IQueryable<TElement>;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = TypeHelper.GetElementType(expression.Type);

            try
            {
                return Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), new object[] { this, expression }) as IQueryable;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            return (TResult)this.Execute(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return this.Execute(expression);
        }

        public abstract object Execute(Expression expression);
        public abstract string GetQueryText(Expression expression);
    }
}