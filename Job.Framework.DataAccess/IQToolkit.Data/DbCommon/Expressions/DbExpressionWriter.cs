using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbExpressionWriter : ExpressionWriter
    {
        private readonly QueryLanguage language;
        private readonly Dictionary<TableAlias, int> aliasMap;

        protected DbExpressionWriter(TextWriter writer, QueryLanguage language) : base(writer)
        {
            this.language = language;
            this.aliasMap = new Dictionary<TableAlias, int>();
        }

        public new static void Write(TextWriter writer, Expression expression)
        {
            Write(writer, null, expression);
        }

        public static void Write(TextWriter writer, QueryLanguage language, Expression expression)
        {
            new DbExpressionWriter(writer, language).Visit(expression);
        }

        public new static string WriteToString(Expression expression)
        {
            return WriteToString(null, expression);
        }

        public static string WriteToString(QueryLanguage language, Expression expression)
        {
            using (var sw = new StringWriter())
            {
                Write(sw, language, expression); return sw.ToString();
            }
        }

        protected override Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return null;
            }

            switch ((DbExpressionType)exp.NodeType)
            {
                case DbExpressionType.Projection: return this.VisitProjection(exp as DbProjectionExpression);
                case DbExpressionType.ClientJoin: return this.VisitClientJoin(exp as DbClientJoinExpression);
                case DbExpressionType.Select: return this.VisitSelect(exp as DbSelectExpression);
                case DbExpressionType.OuterJoined: return this.VisitOuterJoined(exp as DbOuterJoinedExpression);
                case DbExpressionType.Column: return this.VisitColumn(exp as DbColumnExpression);
                case DbExpressionType.Insert:
                case DbExpressionType.Update:
                case DbExpressionType.Delete:
                case DbExpressionType.If:
                case DbExpressionType.Block:
                case DbExpressionType.Declaration: return this.VisitCommand(exp as DbCommandExpression);
                case DbExpressionType.Batch: return this.VisitBatch(exp as DbBatchExpression);
                case DbExpressionType.Function: return this.VisitFunction(exp as DbFunctionExpression);
                case DbExpressionType.Entity: return this.VisitEntity(exp as DbEntityExpression);
                default:
                    {
                        if (exp is DbExpression)
                        {
                            this.Write(this.FormatQuery(exp)); return exp;
                        }
                        else
                        {
                            return base.Visit(exp);
                        }
                    }
            }
        }

        protected void AddAlias(TableAlias alias)
        {
            if (!this.aliasMap.ContainsKey(alias))
            {
                this.aliasMap.Add(alias, this.aliasMap.Count);
            }
        }

        protected virtual Expression VisitProjection(DbProjectionExpression projection)
        {
            this.AddAlias(projection.Select.Alias);
            this.Write("Project(");
            this.WriteLine(Indentation.Inner);
            this.Write("@\"");
            this.Visit(projection.Select);
            this.Write("\",");
            this.WriteLine(Indentation.Same);
            this.Visit(projection.Projector);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(projection.Aggregator);
            this.WriteLine(Indentation.Outer);
            this.Write(")");

            return projection;
        }

        protected virtual Expression VisitClientJoin(DbClientJoinExpression join)
        {
            this.AddAlias(join.Projection.Select.Alias);
            this.Write("ClientJoin(");
            this.WriteLine(Indentation.Inner);
            this.Write("OuterKey(");
            this.VisitExpressionList(join.OuterKey);
            this.Write("),");
            this.WriteLine(Indentation.Same);
            this.Write("InnerKey(");
            this.VisitExpressionList(join.InnerKey);
            this.Write("),");
            this.WriteLine(Indentation.Same);
            this.Visit(join.Projection);
            this.WriteLine(Indentation.Outer);
            this.Write(")");

            return join;
        }

        protected virtual Expression VisitOuterJoined(DbOuterJoinedExpression outer)
        {
            this.Write("Outer(");
            this.WriteLine(Indentation.Inner);
            this.Visit(outer.Test);
            this.Write(", ");
            this.WriteLine(Indentation.Same);
            this.Visit(outer.Expression);
            this.WriteLine(Indentation.Outer);
            this.Write(")");

            return outer;
        }

        protected virtual Expression VisitSelect(DbSelectExpression select)
        {
            this.Write(select.QueryText); return select;
        }

        protected virtual Expression VisitCommand(DbCommandExpression command)
        {
            this.Write(this.FormatQuery(command)); return command;
        }

        protected virtual string FormatQuery(Expression query)
        {
            return SqlFormatter.Format(query, true);
        }

        protected virtual Expression VisitBatch(DbBatchExpression batch)
        {
            this.Write("Batch(");
            this.WriteLine(Indentation.Inner);
            this.Visit(batch.Input);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(batch.Operation);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(batch.BatchSize);
            this.Write(", ");
            this.Visit(batch.Stream);
            this.WriteLine(Indentation.Outer);
            this.Write(")");

            return batch;
        }

        protected virtual Expression VisitVariable(DbVariableExpression vex)
        {
            this.Write(this.FormatQuery(vex)); return vex;
        }

        protected virtual Expression VisitFunction(DbFunctionExpression function)
        {
            this.Write("FUNCTION ");
            this.Write(function.Name);

            if (function.Arguments.Count > 0)
            {
                this.Write("(");
                this.VisitExpressionList(function.Arguments);
                this.Write(")");
            }

            return function;
        }

        protected virtual Expression VisitEntity(DbEntityExpression entity)
        {
            this.Visit(entity.Expression); return entity;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Type == typeof(QueryCommand))
            {
                var qc = c.Value as QueryCommand;

                this.Write("new QueryCommand {");
                this.WriteLine(Indentation.Inner);
                this.Write("\"" + qc.CommandText + "\"");
                this.Write(",");
                this.WriteLine(Indentation.Same);
                this.Visit(Expression.Constant(qc.Parameters));
                this.Write(")");
                this.WriteLine(Indentation.Outer);

                return c;
            }

            return base.VisitConstant(c);
        }

        protected virtual Expression VisitColumn(DbColumnExpression column)
        {
            var aliasName = this.aliasMap.TryGetValue(column.Alias, out int iAlias) ? "A" + iAlias : "A" + (column.Alias != null ? column.Alias.GetHashCode().ToString() : "") + "?";

            this.Write(aliasName);
            this.Write(".");
            this.Write("Column(\"");
            this.Write(column.Name);
            this.Write("\")");

            return column;
        }
    }
}