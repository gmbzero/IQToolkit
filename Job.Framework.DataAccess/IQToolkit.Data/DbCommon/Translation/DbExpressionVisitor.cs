using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal abstract class DbExpressionVisitor : ExpressionVisitor
    {
        protected override Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return null;
            }

            switch ((DbExpressionType)exp.NodeType)
            {
                case DbExpressionType.Table: return this.VisitTable(exp as DbTableExpression);
                case DbExpressionType.Column: return this.VisitColumn(exp as DbColumnExpression);
                case DbExpressionType.Select: return this.VisitSelect(exp as DbSelectExpression);
                case DbExpressionType.Join: return this.VisitJoin(exp as DbJoinExpression);
                case DbExpressionType.OuterJoined: return this.VisitOuterJoined(exp as DbOuterJoinedExpression);
                case DbExpressionType.Aggregate: return this.VisitAggregate(exp as DbAggregateExpression);
                case DbExpressionType.Scalar:
                case DbExpressionType.Exists:
                case DbExpressionType.In: return this.VisitSubquery(exp as SubqueryExpression);
                case DbExpressionType.AggregateSubquery: return this.VisitAggregateSubquery(exp as DbAggregateSubqueryExpression);
                case DbExpressionType.IsNull: return this.VisitIsNull(exp as DbIsNullExpression);
                case DbExpressionType.Between: return this.VisitBetween(exp as DbBetweenExpression);
                case DbExpressionType.RowCount: return this.VisitRowNumber(exp as DbRowNumberExpression);
                case DbExpressionType.Projection: return this.VisitProjection(exp as DbProjectionExpression);
                case DbExpressionType.NamedValue: return this.VisitNamedValue(exp as DbNamedValueExpression);
                case DbExpressionType.ClientJoin: return this.VisitClientJoin(exp as DbClientJoinExpression);
                case DbExpressionType.Insert:
                case DbExpressionType.Update:
                case DbExpressionType.Delete:
                case DbExpressionType.If:
                case DbExpressionType.Block:
                case DbExpressionType.Declaration: return this.VisitCommand(exp as DbCommandExpression);
                case DbExpressionType.Batch: return this.VisitBatch(exp as DbBatchExpression);
                case DbExpressionType.Variable: return this.VisitVariable(exp as DbVariableExpression);
                case DbExpressionType.Function: return this.VisitFunction(exp as DbFunctionExpression);
                case DbExpressionType.Entity: return this.VisitEntity(exp as DbEntityExpression);
                default:
                    {
                        return base.Visit(exp);
                    }
            }
        }

        protected virtual Expression VisitEntity(DbEntityExpression entity)
        {
            return this.UpdateEntity(entity, this.Visit(entity.Expression));
        }

        protected DbEntityExpression UpdateEntity(DbEntityExpression entity, Expression expression)
        {
            if (expression != entity.Expression)
            {
                return new DbEntityExpression(entity.Entity, expression);
            }

            return entity;
        }

        protected virtual Expression VisitTable(DbTableExpression table)
        {
            return table;
        }

        protected virtual Expression VisitColumn(DbColumnExpression column)
        {
            return column;
        }

        protected virtual Expression VisitSelect(DbSelectExpression select)
        {
            var from = this.VisitSource(select.From);
            var where = this.Visit(select.Where);
            var orderBy = this.VisitOrderBy(select.OrderBy);
            var groupBy = this.VisitExpressionList(select.GroupBy);
            var skip = this.Visit(select.Skip);
            var take = this.Visit(select.Take);
            var columns = this.VisitColumnDeclarations(select.Columns);

            return this.UpdateSelect(select, from, where, orderBy, groupBy, skip, take, select.IsDistinct, select.IsReverse, columns);
        }

        protected DbSelectExpression UpdateSelect(DbSelectExpression select, Expression from, Expression where, IEnumerable<DbOrderExpression> orderBy, IEnumerable<Expression> groupBy, Expression skip, Expression take, bool isDistinct, bool isReverse, IEnumerable<DbColumnDeclaration> columns)
        {
            if (from != select.From || where != select.Where || orderBy != select.OrderBy || groupBy != select.GroupBy || take != select.Take || skip != select.Skip || isDistinct != select.IsDistinct || columns != select.Columns || isReverse != select.IsReverse)
            {
                return new DbSelectExpression(select.Alias, columns, from, where, orderBy, groupBy, isDistinct, skip, take, isReverse);
            }

            return select;
        }

        protected virtual Expression VisitJoin(DbJoinExpression join)
        {
            var left = this.VisitSource(join.Left);
            var right = this.VisitSource(join.Right);
            var condition = this.Visit(join.Condition);

            return this.UpdateJoin(join, join.JoinType, left, right, condition);
        }

        protected DbJoinExpression UpdateJoin(DbJoinExpression join, JoinType joinType, Expression left, Expression right, Expression condition)
        {
            if (joinType != join.JoinType || left != join.Left || right != join.Right || condition != join.Condition)
            {
                return new DbJoinExpression(joinType, left, right, condition);
            }

            return join;
        }

        protected virtual Expression VisitOuterJoined(DbOuterJoinedExpression outer)
        {
            var test = this.Visit(outer.Test);
            var expression = this.Visit(outer.Expression);

            return this.UpdateOuterJoined(outer, test, expression);
        }

        protected DbOuterJoinedExpression UpdateOuterJoined(DbOuterJoinedExpression outer, Expression test, Expression expression)
        {
            if (test != outer.Test || expression != outer.Expression)
            {
                return new DbOuterJoinedExpression(test, expression);
            }

            return outer;
        }

        protected virtual Expression VisitAggregate(DbAggregateExpression aggregate)
        {
            return this.UpdateAggregate
            (
                aggregate,
                aggregate.Type,
                aggregate.AggregateName,
                this.Visit(aggregate.Argument),
                aggregate.IsDistinct
            );
        }

        protected DbAggregateExpression UpdateAggregate(DbAggregateExpression aggregate, Type type, string aggType, Expression arg, bool isDistinct)
        {
            if (type != aggregate.Type || aggType != aggregate.AggregateName || arg != aggregate.Argument || isDistinct != aggregate.IsDistinct)
            {
                return new DbAggregateExpression(type, aggType, arg, isDistinct);
            }

            return aggregate;
        }

        protected virtual Expression VisitIsNull(DbIsNullExpression isnull)
        {
            return this.UpdateIsNull(isnull, this.Visit(isnull.Expression));
        }

        protected DbIsNullExpression UpdateIsNull(DbIsNullExpression isnull, Expression expression)
        {
            if (expression != isnull.Expression)
            {
                return new DbIsNullExpression(expression);
            }

            return isnull;
        }

        protected virtual Expression VisitBetween(DbBetweenExpression between)
        {
            var expr = this.Visit(between.Expression);
            var lower = this.Visit(between.Lower);
            var upper = this.Visit(between.Upper);

            return this.UpdateBetween(between, expr, lower, upper);
        }

        protected DbBetweenExpression UpdateBetween(DbBetweenExpression between, Expression expression, Expression lower, Expression upper)
        {
            if (expression != between.Expression || lower != between.Lower || upper != between.Upper)
            {
                return new DbBetweenExpression(expression, lower, upper);
            }

            return between;
        }

        protected virtual Expression VisitRowNumber(DbRowNumberExpression rowNumber)
        {
            return this.UpdateRowNumber(rowNumber, this.VisitOrderBy(rowNumber.OrderBy));
        }

        protected DbRowNumberExpression UpdateRowNumber(DbRowNumberExpression rowNumber, IEnumerable<DbOrderExpression> orderBy)
        {
            if (orderBy != rowNumber.OrderBy)
            {
                return new DbRowNumberExpression(orderBy);
            }

            return rowNumber;
        }

        protected virtual Expression VisitNamedValue(DbNamedValueExpression value)
        {
            return value;
        }

        protected virtual Expression VisitSubquery(SubqueryExpression subquery)
        {
            switch ((DbExpressionType)subquery.NodeType)
            {
                case DbExpressionType.Scalar: return this.VisitScalar(subquery as DbScalarExpression);
                case DbExpressionType.Exists: return this.VisitExists(subquery as DbExistsExpression);
                case DbExpressionType.In: return this.VisitIn(subquery as DbInExpression);
            }

            return subquery;
        }

        protected virtual Expression VisitScalar(DbScalarExpression scalar)
        {
            return this.UpdateScalar(scalar, this.Visit(scalar.Select) as DbSelectExpression);
        }

        protected DbScalarExpression UpdateScalar(DbScalarExpression scalar, DbSelectExpression select)
        {
            if (select != scalar.Select)
            {
                return new DbScalarExpression(scalar.Type, select);
            }

            return scalar;
        }

        protected virtual Expression VisitExists(DbExistsExpression exists)
        {
            return this.UpdateExists(exists, this.Visit(exists.Select) as DbSelectExpression);
        }

        protected DbExistsExpression UpdateExists(DbExistsExpression exists, DbSelectExpression select)
        {
            if (select != exists.Select)
            {
                return new DbExistsExpression(select);
            }

            return exists;
        }

        protected virtual Expression VisitIn(DbInExpression inExp)
        {
            var expr = this.Visit(inExp.Expression);
            var select = this.Visit(inExp.Select) as DbSelectExpression;
            var values = this.VisitExpressionList(inExp.Values);

            return this.UpdateIn(inExp, expr, select, values);
        }

        protected DbInExpression UpdateIn(DbInExpression inExp, Expression expression, DbSelectExpression select, IEnumerable<Expression> values)
        {
            if (expression != inExp.Expression || select != inExp.Select || values != inExp.Values)
            {
                if (select != null)
                {
                    return new DbInExpression(expression, select);
                }
                else
                {
                    return new DbInExpression(expression, values);
                }
            }

            return inExp;
        }

        protected virtual Expression VisitAggregateSubquery(DbAggregateSubqueryExpression aggregate)
        {
            return this.UpdateAggregateSubquery(aggregate, this.Visit(aggregate.AggregateAsSubquery) as DbScalarExpression);
        }

        protected DbAggregateSubqueryExpression UpdateAggregateSubquery(DbAggregateSubqueryExpression aggregate, DbScalarExpression subquery)
        {
            if (subquery != aggregate.AggregateAsSubquery)
            {
                return new DbAggregateSubqueryExpression(aggregate.GroupByAlias, aggregate.AggregateInGroupSelect, subquery);
            }

            return aggregate;
        }

        protected virtual Expression VisitSource(Expression source)
        {
            return this.Visit(source);
        }

        protected virtual Expression VisitProjection(DbProjectionExpression proj)
        {
            var select = this.Visit(proj.Select) as DbSelectExpression;
            var projector = this.Visit(proj.Projector);

            return this.UpdateProjection(proj, select, projector, proj.Aggregator);
        }

        protected DbProjectionExpression UpdateProjection(DbProjectionExpression proj, DbSelectExpression select, Expression projector, LambdaExpression aggregator)
        {
            if (select != proj.Select || projector != proj.Projector || aggregator != proj.Aggregator)
            {
                return new DbProjectionExpression(select, projector, aggregator);
            }

            return proj;
        }

        protected virtual Expression VisitClientJoin(DbClientJoinExpression join)
        {
            var projection = this.Visit(join.Projection) as DbProjectionExpression;
            var outerKey = this.VisitExpressionList(join.OuterKey);
            var innerKey = this.VisitExpressionList(join.InnerKey);

            return this.UpdateClientJoin(join, projection, outerKey, innerKey);
        }

        protected DbClientJoinExpression UpdateClientJoin(DbClientJoinExpression join, DbProjectionExpression projection, IEnumerable<Expression> outerKey, IEnumerable<Expression> innerKey)
        {
            if (projection != join.Projection || outerKey != join.OuterKey || innerKey != join.InnerKey)
            {
                return new DbClientJoinExpression(projection, outerKey, innerKey);
            }

            return join;
        }

        protected virtual Expression VisitCommand(DbCommandExpression command)
        {
            switch ((DbExpressionType)command.NodeType)
            {
                case DbExpressionType.Insert: return this.VisitInsert(command as DbInsertCommand);
                case DbExpressionType.Update: return this.VisitUpdate(command as DbUpdateCommand);
                case DbExpressionType.Delete: return this.VisitDelete(command as DbDeleteCommand);
                case DbExpressionType.If: return this.VisitIf(command as DbIFCommand);
                case DbExpressionType.Block: return this.VisitBlock(command as DbBlockCommand);
                case DbExpressionType.Declaration: return this.VisitDeclaration(command as DbDeclarationCommand);
                default:
                    {
                        return this.VisitUnknown(command);
                    }
            }
        }

        protected virtual Expression VisitInsert(DbInsertCommand insert)
        {
            var table = this.Visit(insert.Table) as DbTableExpression;
            var assignments = this.VisitColumnAssignments(insert.Assignments);

            return this.UpdateInsert(insert, table, assignments);
        }

        protected DbInsertCommand UpdateInsert(DbInsertCommand insert, DbTableExpression table, IEnumerable<DbColumnAssignment> assignments)
        {
            if (table != insert.Table || assignments != insert.Assignments)
            {
                return new DbInsertCommand(table, assignments);
            }

            return insert;
        }

        protected virtual Expression VisitUpdate(DbUpdateCommand update)
        {
            var table = this.Visit(update.Table) as DbTableExpression;
            var where = this.Visit(update.Where);
            var assignments = this.VisitColumnAssignments(update.Assignments);

            return this.UpdateUpdate(update, table, where, assignments);
        }

        protected DbUpdateCommand UpdateUpdate(DbUpdateCommand update, DbTableExpression table, Expression where, IEnumerable<DbColumnAssignment> assignments)
        {
            if (table != update.Table || where != update.Where || assignments != update.Assignments)
            {
                return new DbUpdateCommand(table, where, assignments);
            }

            return update;
        }

        protected virtual Expression VisitDelete(DbDeleteCommand delete)
        {
            var table = this.Visit(delete.Table) as DbTableExpression;
            var where = this.Visit(delete.Where);

            return this.UpdateDelete(delete, table, where);
        }

        protected DbDeleteCommand UpdateDelete(DbDeleteCommand delete, DbTableExpression table, Expression where)
        {
            if (table != delete.Table || where != delete.Where)
            {
                return new DbDeleteCommand(table, where);
            }

            return delete;
        }

        protected virtual Expression VisitBatch(DbBatchExpression batch)
        {
            var operation = this.Visit(batch.Operation) as LambdaExpression;
            var batchSize = this.Visit(batch.BatchSize);
            var stream = this.Visit(batch.Stream);

            return this.UpdateBatch(batch, batch.Input, operation, batchSize, stream);
        }

        protected DbBatchExpression UpdateBatch(DbBatchExpression batch, Expression input, LambdaExpression operation, Expression batchSize, Expression stream)
        {
            if (input != batch.Input || operation != batch.Operation || batchSize != batch.BatchSize || stream != batch.Stream)
            {
                return new DbBatchExpression(input, operation, batchSize, stream);
            }

            return batch;
        }

        protected virtual Expression VisitIf(DbIFCommand ifx)
        {
            var check = this.Visit(ifx.Check);
            var ifTrue = this.Visit(ifx.IfTrue);
            var ifFalse = this.Visit(ifx.IfFalse);

            return this.UpdateIf(ifx, check, ifTrue, ifFalse);
        }

        protected DbIFCommand UpdateIf(DbIFCommand ifx, Expression check, Expression ifTrue, Expression ifFalse)
        {
            if (check != ifx.Check || ifTrue != ifx.IfTrue || ifFalse != ifx.IfFalse)
            {
                return new DbIFCommand(check, ifTrue, ifFalse);
            }

            return ifx;
        }

        protected virtual Expression VisitBlock(DbBlockCommand block)
        {
            return this.UpdateBlock(block, this.VisitExpressionList(block.Commands));
        }

        protected DbBlockCommand UpdateBlock(DbBlockCommand block, IList<Expression> commands)
        {
            if (block.Commands != commands)
            {
                return new DbBlockCommand(commands);
            }

            return block;
        }

        protected virtual Expression VisitDeclaration(DbDeclarationCommand decl)
        {
            var variables = this.VisitVariableDeclarations(decl.Variables);
            var source = this.Visit(decl.Source) as DbSelectExpression;

            return this.UpdateDeclaration(decl, variables, source);

        }

        protected DbDeclarationCommand UpdateDeclaration(DbDeclarationCommand decl, IEnumerable<DbVariableDeclaration> variables, DbSelectExpression source)
        {
            if (variables != decl.Variables || source != decl.Source)
            {
                return new DbDeclarationCommand(variables, source);
            }

            return decl;
        }

        protected virtual Expression VisitVariable(DbVariableExpression vex)
        {
            return vex;
        }

        protected virtual Expression VisitFunction(DbFunctionExpression func)
        {
            return this.UpdateFunction(func, func.Name, this.VisitExpressionList(func.Arguments));
        }

        protected DbFunctionExpression UpdateFunction(DbFunctionExpression func, string name, IEnumerable<Expression> arguments)
        {
            if (name != func.Name || arguments != func.Arguments)
            {
                return new DbFunctionExpression(func.Type, name, arguments);
            }

            return func;
        }

        protected virtual DbColumnAssignment VisitColumnAssignment(DbColumnAssignment ca)
        {
            var c = this.Visit(ca.Column) as DbColumnExpression;
            var e = this.Visit(ca.Expression);

            return this.UpdateColumnAssignment(ca, c, e);
        }

        protected DbColumnAssignment UpdateColumnAssignment(DbColumnAssignment ca, DbColumnExpression c, Expression e)
        {
            if (c != ca.Column || e != ca.Expression)
            {
                return new DbColumnAssignment(c, e);
            }

            return ca;
        }

        protected virtual ReadOnlyCollection<DbColumnAssignment> VisitColumnAssignments(ReadOnlyCollection<DbColumnAssignment> assignments)
        {
            var alternate = null as List<DbColumnAssignment>;

            for (int i = 0, n = assignments.Count; i < n; i++)
            {
                var assignment = this.VisitColumnAssignment(assignments[i]);

                if (alternate == null && assignment != assignments[i])
                {
                    alternate = assignments.Take(i).ToList();
                }

                if (alternate != null)
                {
                    alternate.Add(assignment);
                }
            }

            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }

            return assignments;
        }

        protected virtual ReadOnlyCollection<DbColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<DbColumnDeclaration> columns)
        {
            var alternate = null as List<DbColumnDeclaration>;

            for (int i = 0, n = columns.Count; i < n; i++)
            {
                var column = columns[i];
                var e = this.Visit(column.Expression);

                if (alternate == null && e != column.Expression)
                {
                    alternate = columns.Take(i).ToList();
                }

                if (alternate != null)
                {
                    alternate.Add(new DbColumnDeclaration(column.Name, e, column.QueryType));
                }
            }

            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }

            return columns;
        }

        protected virtual ReadOnlyCollection<DbVariableDeclaration> VisitVariableDeclarations(ReadOnlyCollection<DbVariableDeclaration> decls)
        {
            var alternate = null as List<DbVariableDeclaration>;

            for (int i = 0, n = decls.Count; i < n; i++)
            {
                var decl = decls[i];
                var e = this.Visit(decl.Expression);

                if (alternate == null && e != decl.Expression)
                {
                    alternate = decls.Take(i).ToList();
                }

                if (alternate != null)
                {
                    alternate.Add(new DbVariableDeclaration(decl.Name, decl.QueryType, e));
                }
            }

            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }

            return decls;
        }

        protected virtual ReadOnlyCollection<DbOrderExpression> VisitOrderBy(ReadOnlyCollection<DbOrderExpression> expressions)
        {
            if (expressions != null)
            {
                var  alternate = null as List<DbOrderExpression>;

                for (int i = 0, n = expressions.Count; i < n; i++)
                {
                    var expr = expressions[i];
                    var e = this.Visit(expr.Expression);

                    if (alternate == null && e != expr.Expression)
                    {
                        alternate = expressions.Take(i).ToList();
                    }

                    if (alternate != null)
                    {
                        alternate.Add(new DbOrderExpression(expr.OrderType, e));
                    }
                }

                if (alternate != null)
                {
                    return alternate.AsReadOnly();
                }
            }

            return expressions;
        }
    }
}