using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbSubqueryRemover : DbExpressionVisitor
    {
        private readonly HashSet<DbSelectExpression> selectsToRemove;
        private readonly Dictionary<TableAlias, Dictionary<string, Expression>> map;

        private DbSubqueryRemover(IEnumerable<DbSelectExpression> selectsToRemove)
        {
            this.selectsToRemove = new HashSet<DbSelectExpression>(selectsToRemove);
            this.map = this.selectsToRemove.ToDictionary(d => d.Alias, d => d.Columns.ToDictionary(d2 => d2.Name, d2 => d2.Expression));
        }

        public static DbSelectExpression Remove(DbSelectExpression outerSelect, params DbSelectExpression[] selectsToRemove)
        {
            return Remove(outerSelect, selectsToRemove as IEnumerable<DbSelectExpression>);
        }

        public static DbSelectExpression Remove(DbSelectExpression outerSelect, IEnumerable<DbSelectExpression> selectsToRemove)
        {
            return new DbSubqueryRemover(selectsToRemove).Visit(outerSelect) as DbSelectExpression;
        }

        public static DbProjectionExpression Remove(DbProjectionExpression projection, params DbSelectExpression[] selectsToRemove)
        {
            return Remove(projection, selectsToRemove as IEnumerable<DbSelectExpression>);
        }

        public static DbProjectionExpression Remove(DbProjectionExpression projection, IEnumerable<DbSelectExpression> selectsToRemove)
        {
            return new DbSubqueryRemover(selectsToRemove).Visit(projection) as DbProjectionExpression;
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            if (this.selectsToRemove.Contains(select))
            {
                return this.Visit(select.From);
            }
            else
            {
                return base.VisitSelect(select);
            }
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.map.TryGetValue(column.Alias, out Dictionary<string, Expression> nameMap))
            {
                if (nameMap.TryGetValue(column.Name, out Expression expr))
                {
                    return this.Visit(expr);
                }

                throw new Exception("Reference to undefined column");
            }

            return column;
        }
    }
}