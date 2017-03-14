using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbCrossJoinIsolator : DbExpressionVisitor
    {
        private JoinType? lastJoin;
        private ILookup<TableAlias, DbColumnExpression> columns;
        private readonly Dictionary<DbColumnExpression, DbColumnExpression> map;

        public DbCrossJoinIsolator()
        {
            this.map = new Dictionary<DbColumnExpression, DbColumnExpression>();
        }

        public static Expression Isolate(Expression expression)
        {
            return new DbCrossJoinIsolator().Visit(expression);
        }

        protected override Expression VisitSelect(DbSelectExpression select)
        {
            var saveColumns = this.columns;

            this.columns = DbReferencedColumnGatherer.Gather(select).ToLookup(c => c.Alias);

            var saveLastJoin = this.lastJoin;

            this.lastJoin = null;

            var result = base.VisitSelect(select);

            this.columns = saveColumns;
            this.lastJoin = saveLastJoin;

            return result;
        }

        protected override Expression VisitJoin(DbJoinExpression join)
        {
            var saveLastJoin = this.lastJoin;
            this.lastJoin = join.JoinType;

            join = base.VisitJoin(join) as DbJoinExpression;

            this.lastJoin = saveLastJoin;

            if (this.lastJoin != null && (join.JoinType == JoinType.CrossJoin) != (this.lastJoin == JoinType.CrossJoin))
            {
                return this.MakeSubquery(join);
            }

            return join;
        }

        private bool IsCrossJoin(Expression expression)
        {
            if (expression is DbJoinExpression jex)
            {
                return jex.JoinType == JoinType.CrossJoin;
            }

            return false;
        }

        private Expression MakeSubquery(Expression expression)
        {
            var newAlias = new TableAlias();
            var aliases = DbDeclaredAliasGatherer.Gather(expression);
            var decls = new List<DbColumnDeclaration>();

            foreach (var ta in aliases)
            {
                foreach (var col in this.columns[ta])
                {
                    var name = decls.GetAvailableColumnName(col.Name);

                    var decl = new DbColumnDeclaration(name, col, col.QueryType);

                    decls.Add(decl);

                    var newCol = new DbColumnExpression(col.Type, col.QueryType, newAlias, col.Name);

                    this.map.Add(col, newCol);
                }
            }

            return new DbSelectExpression(newAlias, decls, expression, null);
        }

        protected override Expression VisitColumn(DbColumnExpression column)
        {
            if (this.map.TryGetValue(column, out DbColumnExpression mapped))
            {
                return mapped;
            }

            return column;
        }
    }
}