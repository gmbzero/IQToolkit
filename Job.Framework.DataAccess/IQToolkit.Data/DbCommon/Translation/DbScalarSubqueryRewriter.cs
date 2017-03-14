using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace IQToolkit.Data.Common
{
    internal class DbScalarSubqueryRewriter : DbExpressionVisitor
    {
        private Expression currentFrom;
        private readonly QueryLanguage language;

        public DbScalarSubqueryRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new DbScalarSubqueryRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var saveFrom = this.currentFrom;

            var from = this.VisitSource(select.From);

            this.currentFrom = from;

            var where = this.Visit(select.Where);
            var orderBy = this.VisitOrderBy(select.OrderBy);
            var groupBy = this.VisitExpressionList(select.GroupBy);
            var skip = this.Visit(select.Skip);
            var take = this.Visit(select.Take);
            var columns = this.VisitColumnDeclarations(select.Columns);

            from = this.currentFrom;

            this.currentFrom = saveFrom;

            return this.UpdateSelect(select, from, where, orderBy, groupBy, skip, take, select.IsDistinct, select.IsReverse, columns);
        }

        protected override Expression VisitScalar(DbScalarExpression scalar)
        {
            var select = scalar.Select;
            var colType = this.language.TypeSystem.GetColumnType(scalar.Type);

            if (string.IsNullOrEmpty(select.Columns[0].Name))
            {
                var name = select.Columns.GetAvailableColumnName("scalar");

                select = select.SetColumns(new[] { new DbColumnDeclaration(name, select.Columns[0].Expression, colType) });
            }

            this.currentFrom = new DbJoinExpression(JoinType.OuterApply, this.currentFrom, select, null);

            return new DbColumnExpression(scalar.Type, colType, scalar.Select.Alias, select.Columns[0].Name);
        }
    }
}