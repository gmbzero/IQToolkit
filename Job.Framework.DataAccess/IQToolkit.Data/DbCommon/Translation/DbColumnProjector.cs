using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal sealed class ProjectedColumns
    {
        public Expression Projector { get; }

        public ReadOnlyCollection<DbColumnDeclaration> Columns { get; }

        public ProjectedColumns(Expression projector, ReadOnlyCollection<DbColumnDeclaration> columns)
        {
            this.Projector = projector;
            this.Columns = columns;
        }
    }

    internal class DbColumnProjector : DbExpressionVisitor
    {
        private int iColumn;
        private readonly TableAlias newAlias;
        private readonly QueryLanguage language;
        private readonly HashSet<string> columnNames;
        private readonly HashSet<Expression> candidates;
        private readonly List<DbColumnDeclaration> columns;
        private readonly HashSet<TableAlias> existingAliases;
        private readonly Dictionary<DbColumnExpression, DbColumnExpression> map;

        private DbColumnProjector(QueryLanguage language, Expression expression, IEnumerable<DbColumnDeclaration> existingColumns, TableAlias newAlias, IEnumerable<TableAlias> existingAliases)
        {
            this.language = language;
            this.newAlias = newAlias;
            this.existingAliases = new HashSet<TableAlias>(existingAliases);
            this.map = new Dictionary<DbColumnExpression, DbColumnExpression>();

            if (existingColumns != null)
            {
                this.columns = new List<DbColumnDeclaration>(existingColumns);
                this.columnNames = new HashSet<string>(existingColumns.Select(c => c.Name));
            }
            else
            {
                this.columns = new List<DbColumnDeclaration>();
                this.columnNames = new HashSet<string>();
            }

            this.candidates = Nominator.Nominate(language, expression);
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, Expression expression, IEnumerable<DbColumnDeclaration> existingColumns, TableAlias newAlias, IEnumerable<TableAlias> existingAliases)
        {
            var projector = new DbColumnProjector(language, expression, existingColumns, newAlias, existingAliases);
            var expr = projector.Visit(expression);

            return new ProjectedColumns(expr, projector.columns.AsReadOnly());
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, Expression expression, IEnumerable<DbColumnDeclaration> existingColumns, TableAlias newAlias, params TableAlias[] existingAliases)
        {
            return ProjectColumns(language, expression, existingColumns, newAlias, existingAliases as IEnumerable<TableAlias>);
        }

        protected override Expression Visit(Expression expression)
        {
            if (this.candidates.Contains(expression))
            {
                if (expression.NodeType == (ExpressionType)DbExpressionType.Column)
                {
                    var column = expression as DbColumnExpression;

                    if (this.map.TryGetValue(column, out DbColumnExpression mapped))
                    {
                        return mapped;
                    }

                    foreach (DbColumnDeclaration existingColumn in this.columns)
                    {
                        if (existingColumn.Expression is DbColumnExpression cex && cex.Alias == column.Alias && cex.Name == column.Name)
                        {
                            return new DbColumnExpression(column.Type, column.QueryType, this.newAlias, existingColumn.Name);
                        }
                    }

                    if (this.existingAliases.Contains(column.Alias))
                    {
                        var ordinal = this.columns.Count;
                        var columnName = this.GetUniqueColumnName(column.Name);

                        this.columns.Add(new DbColumnDeclaration(columnName, column, column.QueryType));

                        mapped = new DbColumnExpression(column.Type, column.QueryType, this.newAlias, columnName);

                        this.map.Add(column, mapped);
                        this.columnNames.Add(columnName);

                        return mapped;
                    }

                    return column;
                }
                else
                {
                    var columnName = this.GetNextColumnName();
                    var colType = this.language.TypeSystem.GetColumnType(expression.Type);
                    this.columns.Add(new DbColumnDeclaration(columnName, expression, colType));
                    return new DbColumnExpression(expression.Type, colType, this.newAlias, columnName);
                }
            }
            else
            {
                return base.Visit(expression);
            }
        }

        private bool IsColumnNameInUse(string name)
        {
            return this.columnNames.Contains(name);
        }

        private string GetUniqueColumnName(string name)
        {
            var suffix = 1;
            var baseName = name;

            while (this.IsColumnNameInUse(name))
            {
                name = baseName + (suffix++);
            }

            return name;
        }

        private string GetNextColumnName()
        {
            return this.GetUniqueColumnName("c" + (iColumn++));
        }

        private class Nominator : DbExpressionVisitor
        {
            private bool isBlocked;
            private readonly QueryLanguage language;
            private readonly HashSet<Expression> candidates;

            private Nominator(QueryLanguage language)
            {
                this.language = language;
                this.candidates = new HashSet<Expression>();
                this.isBlocked = false;
            }

            public static HashSet<Expression> Nominate(QueryLanguage language, Expression expression)
            {
                var nominator = new Nominator(language);

                if (nominator != null)
                {
                    nominator.Visit(expression);
                }

                return nominator.candidates;
            }

            protected override Expression Visit(Expression expression)
            {
                if (expression != null)
                {
                    var saveIsBlocked = this.isBlocked;

                    this.isBlocked = false;

                    if (this.language.MustBeColumn(expression))
                    {
                        this.candidates.Add(expression);
                    }
                    else
                    {
                        base.Visit(expression);

                        if (this.isBlocked == false)
                        {
                            if (this.language.CanBeColumn(expression))
                            {
                                this.candidates.Add(expression);
                            }
                            else
                            {
                                this.isBlocked = true;
                            }
                        }

                        this.isBlocked |= saveIsBlocked;
                    }
                }

                return expression;
            }

            protected override Expression VisitProjection(DbProjectionExpression proj)
            {
                this.Visit(proj.Projector);

                return proj;
            }
        }
    }
}