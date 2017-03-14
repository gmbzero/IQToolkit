using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbDeclaredAliasGatherer : DbExpressionVisitor
    {
        private readonly HashSet<TableAlias> aliases;

        private DbDeclaredAliasGatherer()
        {
            this.aliases = new HashSet<TableAlias>();
        }

        public static HashSet<TableAlias> Gather(Expression source)
        {
            var gatherer = new DbDeclaredAliasGatherer();

            if (gatherer != null)
            {
                gatherer.Visit(source);
            }

            return gatherer.aliases;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            this.aliases.Add(select.Alias);

            return select;
        }

        protected override Expression VisitTable(DbTableExpression table)
        {
            this.aliases.Add(table.Alias);

            return table;
        }
    }
}