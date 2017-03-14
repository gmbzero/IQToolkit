using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbSkipToRowNumberRewriter : DbExpressionVisitor
    {
        private readonly QueryLanguage language;

        private DbSkipToRowNumberRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbSkipToRowNumberRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            select = base.VisitSelect(select) as DbSelectExpression;

            if (select.Skip != null)
            {
                var newSelect = select.SetSkip(null).SetTake(null);
                var canAddColumn = !select.IsDistinct && (select.GroupBy == null || select.GroupBy.Count == 0);

                if (canAddColumn == false)
                {
                    newSelect = newSelect.AddRedundantSelect(this.language, new TableAlias());
                }

                var colType = this.language.TypeSystem.GetColumnType(typeof(int));

                newSelect = newSelect.AddColumn(new DbColumnDeclaration("_rownum", new DbRowNumberExpression(select.OrderBy), colType));

                newSelect = newSelect.AddRedundantSelect(this.language, new TableAlias());
                newSelect = newSelect.RemoveColumn(newSelect.Columns.Single(c => c.Name == "_rownum"));

                var newAlias = (newSelect.From as DbSelectExpression).Alias;
                var rnCol = new DbColumnExpression(typeof(int), colType, newAlias, "_rownum");
                var where = null as Expression;

                if (select.Take != null)
                {
                    where = new DbBetweenExpression(rnCol, Expression.Add(select.Skip, Expression.Constant(1)), Expression.Add(select.Skip, select.Take));
                }
                else
                {
                    where = rnCol.GreaterThan(select.Skip);
                }

                if (newSelect.Where != null)
                {
                    where = newSelect.Where.And(where);
                }

                newSelect = newSelect.SetWhere(where);

                select = newSelect;
            }

            return select;
        }
    }
}