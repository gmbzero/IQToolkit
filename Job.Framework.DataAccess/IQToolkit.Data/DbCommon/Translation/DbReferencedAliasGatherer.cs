using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbReferencedAliasGatherer : DbExpressionVisitor
    {
        private readonly HashSet<TableAlias> aliases;

        private DbReferencedAliasGatherer()
        {
            this.aliases = new HashSet<TableAlias>();
        }

        public static HashSet<TableAlias> Gather(Expression source)
        {
            var gatherer = new DbReferencedAliasGatherer();

            if (gatherer != null)
            {
                gatherer.Visit(source);
            }

            return gatherer.aliases;
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            this.aliases.Add(column.Alias);

            return column;
        }
    }
}