using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbColumnMapper : DbExpressionVisitor
    {
        private readonly HashSet<TableAlias> oldAliases;
        private readonly TableAlias newAlias;

        private DbColumnMapper(IEnumerable<TableAlias> oldAliases, TableAlias newAlias)
        {
            this.oldAliases = new HashSet<TableAlias>(oldAliases);
            this.newAlias = newAlias;
        }

        public static Expression Map(Expression expression, TableAlias newAlias, IEnumerable<TableAlias> oldAliases)
        {
            return new DbColumnMapper(oldAliases, newAlias).Visit(expression);
        }

        public static Expression Map(Expression expression, TableAlias newAlias, params TableAlias[] oldAliases)
        {
            return Map(expression, newAlias, oldAliases as IEnumerable<TableAlias>);
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.oldAliases.Contains(column.Alias))
            {
                return new DbColumnExpression(column.Type, column.QueryType, this.newAlias, column.Name);
            }

            return column;
        }
    }
}