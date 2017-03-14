using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal static class DbExpressionExtensions
    {
        public static bool IsDbExpression(ExpressionType nodeType)
        {
            return (int)nodeType >= (int)DbExpressionType.Table;
        }

        public static DbSelectExpression SetColumns(this DbSelectExpression select, IEnumerable<DbColumnDeclaration> columns)
        {
            return new DbSelectExpression
            (
                select.Alias,
                columns.OrderBy(c => c.Name),
                select.From,
                select.Where,
                select.OrderBy,
                select.GroupBy,
                select.IsDistinct,
                select.Skip,
                select.Take,
                select.IsReverse
            );
        }

        public static DbSelectExpression AddColumn(this DbSelectExpression select, DbColumnDeclaration column)
        {
            var columns = new List<DbColumnDeclaration>(select.Columns)
            {
                column
            };

            return select.SetColumns(columns);
        }

        public static DbSelectExpression RemoveColumn(this DbSelectExpression select, DbColumnDeclaration column)
        {
            var columns = new List<DbColumnDeclaration>(select.Columns);

            if (columns != null)
            {
                columns.Remove(column);
            }

            return select.SetColumns(columns);
        }

        public static string GetAvailableColumnName(this IList<DbColumnDeclaration> columns, string baseName)
        {
            var name = baseName;
            var n = 0;

            while (IsUniqueName(columns, name) == false)
            {
                name = baseName + (n++);
            }

            return name;
        }

        private static bool IsUniqueName(IList<DbColumnDeclaration> columns, string name)
        {
            foreach (var col in columns)
            {
                if (col.Name == name)
                {
                    return false;
                }
            }

            return true;
        }

        public static DbProjectionExpression AddOuterJoinTest(this DbProjectionExpression proj, QueryLanguage language, Expression expression)
        {
            var colName = proj.Select.Columns.GetAvailableColumnName("Test");
            var colType = language.TypeSystem.GetColumnType(expression.Type);
            var newSource = proj.Select.AddColumn(new DbColumnDeclaration(colName, expression, colType));

            var newProjector = new DbOuterJoinedExpression
            (
                new DbColumnExpression(expression.Type, colType, newSource.Alias, colName),
                proj.Projector
            );

            return new DbProjectionExpression(newSource, newProjector, proj.Aggregator);
        }

        public static DbSelectExpression SetDistinct(this DbSelectExpression select, bool isDistinct)
        {
            if (select.IsDistinct != isDistinct)
            {
                return new DbSelectExpression
                (
                    select.Alias,
                    select.Columns,
                    select.From,
                    select.Where,
                    select.OrderBy,
                    select.GroupBy,
                    isDistinct,
                    select.Skip,
                    select.Take,
                    select.IsReverse
                );
            }

            return select;
        }

        public static DbSelectExpression SetReverse(this DbSelectExpression select, bool isReverse)
        {
            if (select.IsReverse != isReverse)
            {
                return new DbSelectExpression
                (
                    select.Alias,
                    select.Columns,
                    select.From,
                    select.Where,
                    select.OrderBy,
                    select.GroupBy,
                    select.IsDistinct,
                    select.Skip,
                    select.Take,
                    isReverse
                );
            }

            return select;
        }

        public static DbSelectExpression SetWhere(this DbSelectExpression select, Expression where)
        {
            if (where != select.Where)
            {
                return new DbSelectExpression
                (
                    select.Alias,
                    select.Columns,
                    select.From,
                    where,
                    select.OrderBy,
                    select.GroupBy,
                    select.IsDistinct,
                    select.Skip,
                    select.Take,
                    select.IsReverse
                );
            }

            return select;
        }

        public static DbSelectExpression SetOrderBy(this DbSelectExpression select, IEnumerable<DbOrderExpression> orderBy)
        {
            return new DbSelectExpression
            (
                select.Alias,
                select.Columns,
                select.From,
                select.Where,
                orderBy,
                select.GroupBy,
                select.IsDistinct,
                select.Skip,
                select.Take,
                select.IsReverse
            );
        }

        public static DbSelectExpression AddOrderExpression(this DbSelectExpression select, DbOrderExpression ordering)
        {
            var orderby = new List<DbOrderExpression>();

            if (select.OrderBy != null)
            {
                orderby.AddRange(select.OrderBy);
            }

            if (orderby != null)
            {
                orderby.Add(ordering);
            }

            return select.SetOrderBy(orderby);
        }

        public static DbSelectExpression RemoveOrderExpression(this DbSelectExpression select, DbOrderExpression ordering)
        {
            if (select.OrderBy != null && select.OrderBy.Count > 0)
            {
                var orderby = new List<DbOrderExpression>(select.OrderBy);

                if (orderby != null)
                {
                    orderby.Remove(ordering);
                }

                return select.SetOrderBy(orderby);
            }

            return select;
        }

        public static DbSelectExpression SetGroupBy(this DbSelectExpression select, IEnumerable<Expression> groupBy)
        {
            return new DbSelectExpression
            (
                select.Alias,
                select.Columns,
                select.From,
                select.Where,
                select.OrderBy,
                groupBy,
                select.IsDistinct,
                select.Skip,
                select.Take,
                select.IsReverse
            );
        }

        public static DbSelectExpression AddGroupExpression(this DbSelectExpression select, Expression expression)
        {
            var groupby = new List<Expression>();

            if (select.GroupBy != null)
            {
                groupby.AddRange(select.GroupBy);
            }

            if (groupby != null)
            {
                groupby.Add(expression);
            }

            return select.SetGroupBy(groupby);
        }

        public static DbSelectExpression RemoveGroupExpression(this DbSelectExpression select, Expression expression)
        {
            if (select.GroupBy != null && select.GroupBy.Count > 0)
            {
                var groupby = new List<Expression>(select.GroupBy);

                if (groupby != null)
                {
                    groupby.Remove(expression);
                }

                return select.SetGroupBy(groupby);
            }

            return select;
        }

        public static DbSelectExpression SetSkip(this DbSelectExpression select, Expression skip)
        {
            if (skip != select.Skip)
            {
                return new DbSelectExpression
                (
                    select.Alias,
                    select.Columns,
                    select.From,
                    select.Where,
                    select.OrderBy,
                    select.GroupBy,
                    select.IsDistinct,
                    skip,
                    select.Take,
                    select.IsReverse
                );
            }

            return select;
        }

        public static DbSelectExpression SetTake(this DbSelectExpression select, Expression take)
        {
            if (take != select.Take)
            {
                return new DbSelectExpression
                (
                    select.Alias,
                    select.Columns,
                    select.From,
                    select.Where,
                    select.OrderBy,
                    select.GroupBy,
                    select.IsDistinct,
                    select.Skip,
                    take,
                    select.IsReverse
                );
            }

            return select;
        }

        public static DbSelectExpression AddRedundantSelect(this DbSelectExpression sel, QueryLanguage language, TableAlias newAlias)
        {
            var newColumns =
            (
                from d in sel.Columns
                let qt = (d.Expression is DbColumnExpression) ? (d.Expression as DbColumnExpression).QueryType : language.TypeSystem.GetColumnType(d.Expression.Type)
                select new DbColumnDeclaration(d.Name, new DbColumnExpression(d.Expression.Type, qt, newAlias, d.Name), qt)
            );

            var newFrom = new DbSelectExpression
            (
                newAlias,
                sel.Columns,
                sel.From,
                sel.Where,
                sel.OrderBy,
                sel.GroupBy,
                sel.IsDistinct,
                sel.Skip,
                sel.Take,
                sel.IsReverse
            );

            return new DbSelectExpression
            (
                sel.Alias,
                newColumns,
                newFrom,
                null, null, null, false, null, null, false
            );
        }

        public static DbSelectExpression RemoveRedundantFrom(this DbSelectExpression select)
        {
            if (select.From is DbSelectExpression fromSelect)
            {
                return DbSubqueryRemover.Remove(select, fromSelect);
            }

            return select;
        }

        public static DbSelectExpression SetFrom(this DbSelectExpression select, Expression from)
        {
            if (select.From != from)
            {
                return new DbSelectExpression
                (
                    select.Alias, 
                    select.Columns,
                    from, 
                    select.Where, 
                    select.OrderBy, 
                    select.GroupBy, 
                    select.IsDistinct,
                    select.Skip,
                    select.Take, 
                    select.IsReverse
                );
            }

            return select;
        }
    }
}