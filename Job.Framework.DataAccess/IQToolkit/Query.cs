using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    internal interface IQueryText
    {
        string GetQueryText(Expression expression);
    }

    internal class Query<T> : IQueryable<T>, IQueryable, IEnumerableAsync<T>, IEnumerable<T>, IEnumerable, IOrderedQueryable<T>, IOrderedQueryable
    {
        public Expression Expression { get; }

        public Type ElementType { get; }

        public IQueryProvider Provider { get; }

        public Query(IQueryProvider provider) : this(provider, null as Type)
        {

        }

        public Query(IQueryProvider provider, Type staticType)
        {
            this.Provider = provider ?? throw new ArgumentNullException("Provider");
            this.Expression = staticType != null ? Expression.Constant(this, staticType) : Expression.Constant(this);
            this.ElementType = typeof(T);
        }

        public Query(IQueryProvider provider, Expression expression)
        {
            this.Provider = provider ?? throw new ArgumentNullException("Provider");
            this.Expression = expression ?? throw new ArgumentNullException("expression");
            this.ElementType = typeof(T);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return (this.Provider.Execute<IEnumerable<T>>(this.Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return (this.Provider.Execute<IEnumerable>(this.Expression)).GetEnumerator();
        }

        public Task<IEnumeratorAsync<T>> GetEnumeratorAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = this.Provider.Execute<IEnumerable<T>>(this.Expression);

            return result.ToAsync().GetEnumeratorAsync(cancellationToken);
        }

        public override string ToString()
        {
            if (this.Expression.NodeType == ExpressionType.Constant && (this.Expression as ConstantExpression).Value == this)
            {
                return "Query(" + typeof(T) + ")";
            }
            else
            {
                return this.Expression.ToString();
            }
        }

        public string QueryText
        {
            get
            {
                if (this.Provider is IQueryText iqt)
                {
                    return iqt.GetQueryText(this.Expression);
                }

                return string.Empty;
            }
        }
    }
}