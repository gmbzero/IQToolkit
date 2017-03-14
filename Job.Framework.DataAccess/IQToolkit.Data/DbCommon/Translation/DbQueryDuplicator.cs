using System.Collections.Generic;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbQueryDuplicator : DbExpressionVisitor
    {
        private readonly Dictionary<TableAlias, TableAlias> map;

        public DbQueryDuplicator()
        {
            this.map = new Dictionary<TableAlias, TableAlias>();
        }

        public static Expression Duplicate(Expression expression)
        {
            return new DbQueryDuplicator().Visit(expression);
        }

        protected override Expression VisitTable(DbTableExpression table)
        {
            var newAlias = new TableAlias();
            this.map[table.Alias] = newAlias;

            return new DbTableExpression(newAlias, table.Entity, table.Name);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var newAlias = new TableAlias();
            this.map[select.Alias] = newAlias;

            select = base.VisitSelect(select) as DbSelectExpression;

            return new DbSelectExpression(newAlias, select.Columns, select.From, select.Where, select.OrderBy, select.GroupBy, select.IsDistinct, select.Skip, select.Take, select.IsReverse);
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.map.TryGetValue(column.Alias, out TableAlias newAlias))
            {
                return new DbColumnExpression(column.Type, column.QueryType, newAlias, column.Name);
            }

            return column;
        }
    }
}