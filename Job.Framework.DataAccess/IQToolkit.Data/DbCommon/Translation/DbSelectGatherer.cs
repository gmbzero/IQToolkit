using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbSelectGatherer : DbExpressionVisitor
    {
        private readonly List<DbSelectExpression> selects;

        public DbSelectGatherer()
        {
            this.selects = new List<DbSelectExpression>();
        }

        public static ReadOnlyCollection<DbSelectExpression> Gather(Expression expression)
        {
            var gatherer = new DbSelectGatherer();

            if (gatherer != null)
            {
                gatherer.Visit(expression);
            }

            return gatherer.selects.AsReadOnly();
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            this.selects.Add(select);

            return select;
        }
    }
}