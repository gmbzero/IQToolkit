using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    internal class QueryResult<T> : IOrderedQueryable<T>, IEnumerableAsync<T>
    {
        private readonly ResultProvider provider;
        private readonly IEnumerable<T> enumerable;

        public Expression Expression
        {
            get { return Expression.Constant(this); }
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public IQueryProvider Provider
        {
            get { return provider; }
        }

        public QueryResult(IEnumerable<T> enumerable)
        {
            this.enumerable = enumerable;
            this.provider = new ResultProvider();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return this.enumerable.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public Task<IEnumeratorAsync<T>> GetEnumeratorAsync(CancellationToken cancellationToken)
        {
            return this.enumerable.ToAsync().GetEnumeratorAsync(cancellationToken);
        }

        private class ResultProvider : QueryProvider
        {
            public override object Execute(Expression expression)
            {
                var localExpression = MethodCallRewriter.Rewrite(expression);
                var efn = Expression.Lambda<Func<object>>(Expression.Convert(localExpression, typeof(object)));
                var fn = efn.Compile();

                return fn();
            }

            public override string GetQueryText(Expression expression)
            {
                return ExpressionWriter.WriteToString(expression);
            }
        }

        private class MethodCallRewriter : ExpressionVisitor
        {
            public static Expression Rewrite(Expression expression)
            {
                return new MethodCallRewriter().Visit(expression);
            }

            protected override Expression VisitMethodCall(MethodCallExpression m)
            {
                if (m.Object == null)
                {
                    var rewrittenArgs = this.VisitExpressionList(m.Arguments);

                    if (m.Method.DeclaringType == typeof(Queryable))
                    {
                        return Expression.Call(typeof(Enumerable), m.Method.Name, m.Method.GetGenericArguments(), rewrittenArgs.ToArray());
                    }
                    else if (rewrittenArgs != m.Arguments)
                    {
                        return Expression.Call(m.Method.DeclaringType, m.Method.Name, m.Method.GetGenericArguments(), rewrittenArgs.ToArray());
                    }
                    else
                    {
                        return m;
                    }
                }
                else
                {
                    return base.VisitMethodCall(m);
                }
            }
        }
    }
}