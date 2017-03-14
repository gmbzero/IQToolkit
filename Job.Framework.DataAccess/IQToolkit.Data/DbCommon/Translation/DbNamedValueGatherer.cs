using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbNamedValueGatherer : DbExpressionVisitor
    {
        private readonly HashSet<DbNamedValueExpression> namedValues;

        public DbNamedValueGatherer()
        {
            this.namedValues = new HashSet<DbNamedValueExpression>(new NamedValueComparer());
        }

        public static ReadOnlyCollection<DbNamedValueExpression> Gather(Expression expr)
        {
            var gatherer = new DbNamedValueGatherer();

            if (gatherer != null)
            {
                gatherer.Visit(expr);
            }

            return gatherer.namedValues.ToList().AsReadOnly();
        }

        protected override Expression VisitNamedValue(DbNamedValueExpression value)
        {
            this.namedValues.Add(value);

            return value;
        }

        private class NamedValueComparer : IEqualityComparer<DbNamedValueExpression>
        {
            public bool Equals(DbNamedValueExpression x, DbNamedValueExpression y)
            {
                return x.Name == y.Name;
            }

            public int GetHashCode(DbNamedValueExpression obj)
            {
                return obj.Name.GetHashCode();
            }
        }
    }
}