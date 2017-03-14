using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbExpressionComparer : ExpressionComparer
    {
        private ScopedDictionary<TableAlias, TableAlias> aliasScope;

        protected DbExpressionComparer(ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope, Func<object, object, bool> fnCompare, ScopedDictionary<TableAlias, TableAlias> aliasScope) : base(parameterScope, fnCompare)
        {
            this.aliasScope = aliasScope;
        }

        public new static bool AreEqual(Expression a, Expression b)
        {
            return AreEqual(null, null, a, b, null);
        }

        public new static bool AreEqual(Expression a, Expression b, Func<object, object, bool> fnCompare)
        {
            return AreEqual(null, null, a, b, fnCompare);
        }

        public static bool AreEqual(ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope, ScopedDictionary<TableAlias, TableAlias> aliasScope, Expression a, Expression b)
        {
            return new DbExpressionComparer(parameterScope, null, aliasScope).Compare(a, b);
        }

        public static bool AreEqual(ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope, ScopedDictionary<TableAlias, TableAlias> aliasScope, Expression a, Expression b, Func<object, object, bool> fnCompare)
        {
            return new DbExpressionComparer(parameterScope, fnCompare, aliasScope).Compare(a, b);
        }

        protected override bool Compare(Expression a, Expression b)
        {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a.NodeType != b.NodeType) return false;
            if (a.Type != b.Type) return false;

            switch ((DbExpressionType)a.NodeType)
            {
                case DbExpressionType.Table: return this.CompareTable(a as DbTableExpression, b as DbTableExpression);
                case DbExpressionType.Column: return this.CompareColumn(a as DbColumnExpression, b as DbColumnExpression);
                case DbExpressionType.Select: return this.CompareSelect(a as DbSelectExpression, b as DbSelectExpression);
                case DbExpressionType.Join: return this.CompareJoin(a as DbJoinExpression, b as DbJoinExpression);
                case DbExpressionType.Aggregate: return this.CompareAggregate(a as DbAggregateExpression, b as DbAggregateExpression);
                case DbExpressionType.Scalar:
                case DbExpressionType.Exists:
                case DbExpressionType.In: return this.CompareSubquery(a as SubqueryExpression, b as SubqueryExpression);
                case DbExpressionType.AggregateSubquery: return this.CompareAggregateSubquery(a as DbAggregateSubqueryExpression, b as DbAggregateSubqueryExpression);
                case DbExpressionType.IsNull: return this.CompareIsNull(a as DbIsNullExpression, b as DbIsNullExpression);
                case DbExpressionType.Between: return this.CompareBetween(a as DbBetweenExpression, b as DbBetweenExpression);
                case DbExpressionType.RowCount: return this.CompareRowNumber(a as DbRowNumberExpression, b as DbRowNumberExpression);
                case DbExpressionType.Projection: return this.CompareProjection(a as DbProjectionExpression, b as DbProjectionExpression);
                case DbExpressionType.NamedValue: return this.CompareNamedValue(a as DbNamedValueExpression, b as DbNamedValueExpression);
                case DbExpressionType.Insert: return this.CompareInsert(a as DbInsertCommand, b as DbInsertCommand);
                case DbExpressionType.Update: return this.CompareUpdate(a as DbUpdateCommand, b as DbUpdateCommand);
                case DbExpressionType.Delete: return this.CompareDelete(a as DbDeleteCommand, b as DbDeleteCommand);
                case DbExpressionType.Batch: return this.CompareBatch(a as DbBatchExpression, b as DbBatchExpression);
                case DbExpressionType.Function: return this.CompareFunction(a as DbFunctionExpression, b as DbFunctionExpression);
                case DbExpressionType.Entity: return this.CompareEntity(a as DbEntityExpression, b as DbEntityExpression);
                case DbExpressionType.If: return this.CompareIf(a as DbIFCommand, b as DbIFCommand);
                case DbExpressionType.Block: return this.CompareBlock(a as DbBlockCommand, b as DbBlockCommand);
                default:
                    {
                        return base.Compare(a, b);
                    }
            }
        }

        protected virtual bool CompareTable(DbTableExpression a, DbTableExpression b)
        {
            return a.Name == b.Name;
        }

        protected virtual bool CompareColumn(DbColumnExpression a, DbColumnExpression b)
        {
            return this.CompareAlias(a.Alias, b.Alias) && a.Name == b.Name;
        }

        protected virtual bool CompareAlias(TableAlias a, TableAlias b)
        {
            if (this.aliasScope != null)
            {
                if (this.aliasScope.TryGetValue(a, out TableAlias mapped))
                {
                    return mapped == b;
                }
            }

            return a == b;
        }

        protected virtual bool CompareSelect(DbSelectExpression a, DbSelectExpression b)
        {
            var save = this.aliasScope;

            try
            {
                if (this.Compare(a.From, b.From) == false)
                {
                    return false;
                }

                this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(save);

                this.MapAliases(a.From, b.From);

                return
                (
                    this.Compare(a.Where, b.Where)
                    && this.CompareOrderList(a.OrderBy, b.OrderBy)
                    && this.CompareExpressionList(a.GroupBy, b.GroupBy)
                    && this.Compare(a.Skip, b.Skip)
                    && this.Compare(a.Take, b.Take)
                    && a.IsDistinct == b.IsDistinct
                    && a.IsReverse == b.IsReverse
                    && this.CompareColumnDeclarations(a.Columns, b.Columns)
                );
            }
            finally
            {
                this.aliasScope = save;
            }
        }

        private void MapAliases(Expression a, Expression b)
        {
            var prodA = DbDeclaredAliasGatherer.Gather(a).ToArray();
            var prodB = DbDeclaredAliasGatherer.Gather(b).ToArray();

            for (int i = 0, n = prodA.Length; i < n; i++)
            {
                this.aliasScope.Add(prodA[i], prodB[i]);
            }
        }

        protected virtual bool CompareOrderList(ReadOnlyCollection<DbOrderExpression> a, ReadOnlyCollection<DbOrderExpression> b)
        {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;

            for (int i = 0, n = a.Count; i < n; i++)
            {
                if (a[i].OrderType != b[i].OrderType || !this.Compare(a[i].Expression, b[i].Expression))
                {
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CompareColumnDeclarations(ReadOnlyCollection<DbColumnDeclaration> a, ReadOnlyCollection<DbColumnDeclaration> b)
        {
            if (a == b) return true;
            if (a == null || b == null) return false;
            if (a.Count != b.Count) return false;

            for (int i = 0, n = a.Count; i < n; i++)
            {
                if (this.CompareColumnDeclaration(a[i], b[i]) == false)
                {
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CompareColumnDeclaration(DbColumnDeclaration a, DbColumnDeclaration b)
        {
            return a.Name == b.Name && this.Compare(a.Expression, b.Expression);
        }

        protected virtual bool CompareJoin(DbJoinExpression a, DbJoinExpression b)
        {
            if (a.JoinType != b.JoinType || !this.Compare(a.Left, b.Left))
            {
                return false;
            }

            if (a.JoinType == JoinType.CrossApply || a.JoinType == JoinType.OuterApply)
            {
                var save = this.aliasScope;

                try
                {
                    this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(this.aliasScope);

                    this.MapAliases(a.Left, b.Left);

                    return this.Compare(a.Right, b.Right) && this.Compare(a.Condition, b.Condition);
                }
                finally
                {
                    this.aliasScope = save;
                }
            }
            else
            {
                return this.Compare(a.Right, b.Right) && this.Compare(a.Condition, b.Condition);
            }
        }

        protected virtual bool CompareAggregate(DbAggregateExpression a, DbAggregateExpression b)
        {
            return a.AggregateName == b.AggregateName && this.Compare(a.Argument, b.Argument);
        }

        protected virtual bool CompareIsNull(DbIsNullExpression a, DbIsNullExpression b)
        {
            return this.Compare(a.Expression, b.Expression);
        }

        protected virtual bool CompareBetween(DbBetweenExpression a, DbBetweenExpression b)
        {
            return
            (
                this.Compare(a.Expression, b.Expression)
                && this.Compare(a.Lower, b.Lower)
                && this.Compare(a.Upper, b.Upper)
            );
        }

        protected virtual bool CompareRowNumber(DbRowNumberExpression a, DbRowNumberExpression b)
        {
            return this.CompareOrderList(a.OrderBy, b.OrderBy);
        }

        protected virtual bool CompareNamedValue(DbNamedValueExpression a, DbNamedValueExpression b)
        {
            return a.Name == b.Name && this.Compare(a.Value, b.Value);
        }

        protected virtual bool CompareSubquery(SubqueryExpression a, SubqueryExpression b)
        {
            if (a.NodeType != b.NodeType)
            {
                return false;
            }

            switch ((DbExpressionType)a.NodeType)
            {
                case DbExpressionType.In: return this.CompareIn(a as DbInExpression, b as DbInExpression);
                case DbExpressionType.Scalar: return this.CompareScalar(a as DbScalarExpression, b as DbScalarExpression);
                case DbExpressionType.Exists: return this.CompareExists(a as DbExistsExpression, b as DbExistsExpression);
            }

            return false;
        }

        protected virtual bool CompareScalar(DbScalarExpression a, DbScalarExpression b)
        {
            return this.Compare(a.Select, b.Select);
        }

        protected virtual bool CompareExists(DbExistsExpression a, DbExistsExpression b)
        {
            return this.Compare(a.Select, b.Select);
        }

        protected virtual bool CompareIn(DbInExpression a, DbInExpression b)
        {
            return
            (
                this.Compare(a.Expression, b.Expression)
                && this.Compare(a.Select, b.Select)
                && this.CompareExpressionList(a.Values, b.Values)
            );
        }

        protected virtual bool CompareAggregateSubquery(DbAggregateSubqueryExpression a, DbAggregateSubqueryExpression b)
        {
            return
            (
                this.Compare(a.AggregateAsSubquery, b.AggregateAsSubquery)
                && this.Compare(a.AggregateInGroupSelect, b.AggregateInGroupSelect)
                && a.GroupByAlias == b.GroupByAlias
            );
        }

        protected virtual bool CompareProjection(DbProjectionExpression a, DbProjectionExpression b)
        {
            if (!this.Compare(a.Select, b.Select))
            {
                return false;
            }

            var save = this.aliasScope;

            try
            {
                this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(this.aliasScope);
                this.aliasScope.Add(a.Select.Alias, b.Select.Alias);

                return
                (
                    this.Compare(a.Projector, b.Projector)
                    && this.Compare(a.Aggregator, b.Aggregator)
                    && a.IsSingleton == b.IsSingleton
                );
            }
            finally
            {
                this.aliasScope = save;
            }
        }

        protected virtual bool CompareInsert(DbInsertCommand x, DbInsertCommand y)
        {
            return this.Compare(x.Table, y.Table) && this.CompareColumnAssignments(x.Assignments, y.Assignments);
        }

        protected virtual bool CompareColumnAssignments(ReadOnlyCollection<DbColumnAssignment> x, ReadOnlyCollection<DbColumnAssignment> y)
        {
            if (x == y) return true;
            if (x.Count != y.Count) return false;

            for (int i = 0, n = x.Count; i < n; i++)
            {
                if (!this.Compare(x[i].Column, y[i].Column) || !this.Compare(x[i].Expression, y[i].Expression))
                {
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CompareUpdate(DbUpdateCommand x, DbUpdateCommand y)
        {
            return this.Compare(x.Table, y.Table) && this.Compare(x.Where, y.Where) && this.CompareColumnAssignments(x.Assignments, y.Assignments);
        }

        protected virtual bool CompareDelete(DbDeleteCommand x, DbDeleteCommand y)
        {
            return this.Compare(x.Table, y.Table) && this.Compare(x.Where, y.Where);
        }

        protected virtual bool CompareBatch(DbBatchExpression x, DbBatchExpression y)
        {
            return
            (
                this.Compare(x.Input, y.Input)
                && this.Compare(x.Operation, y.Operation)
                && this.Compare(x.BatchSize, y.BatchSize)
                && this.Compare(x.Stream, y.Stream)
            );
        }

        protected virtual bool CompareIf(DbIFCommand x, DbIFCommand y)
        {
            return this.Compare(x.Check, y.Check) && this.Compare(x.IfTrue, y.IfTrue) && this.Compare(x.IfFalse, y.IfFalse);
        }

        protected virtual bool CompareBlock(DbBlockCommand x, DbBlockCommand y)
        {
            if (x.Commands.Count != y.Commands.Count)
            {
                return false;
            }

            for (int i = 0, n = x.Commands.Count; i < n; i++)
            {
                if (this.Compare(x.Commands[i], y.Commands[i]) == false)
                {
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CompareFunction(DbFunctionExpression x, DbFunctionExpression y)
        {
            return x.Name == y.Name && this.CompareExpressionList(x.Arguments, y.Arguments);
        }

        protected virtual bool CompareEntity(DbEntityExpression x, DbEntityExpression y)
        {
            return x.Entity == y.Entity && this.Compare(x.Expression, y.Expression);
        }
    }
}