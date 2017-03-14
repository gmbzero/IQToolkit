using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal class DbTableExpression : DbAliasedExpression
    {
        public MappingEntity Entity { get; }

        public string Name { get; }


        public DbTableExpression(TableAlias alias, MappingEntity entity, string name) : base(DbExpressionType.Table, typeof(void), alias)
        {
            this.Entity = entity;
            this.Name = name;
        }

        public override string ToString()
        {
            return "T(" + this.Name + ")";
        }
    }

    internal class DbEntityExpression : DbExpression
    {
        public MappingEntity Entity { get; }

        public Expression Expression { get; }

        public DbEntityExpression(MappingEntity entity, Expression expression) : base(DbExpressionType.Entity, expression.Type)
        {
            this.Entity = entity;
            this.Expression = expression;
        }
    }

    internal class DbColumnExpression : DbExpression, IEquatable<DbColumnExpression>
    {
        public TableAlias Alias { get; }

        public QueryType QueryType { get; }

        public string Name { get; }

        public DbColumnExpression(Type type, QueryType queryType, TableAlias alias, string name) : base(DbExpressionType.Column, type)
        {
            this.Alias = alias;
            this.QueryType = queryType ?? throw new ArgumentNullException("queryType");
            this.Name = name ?? throw new ArgumentNullException("name");
        }

        public override string ToString()
        {
            return this.Alias.ToString() + ".C(" + this.Name + ")";
        }

        public override int GetHashCode()
        {
            return this.Alias.GetHashCode() + this.Name.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as DbColumnExpression);
        }

        public bool Equals(DbColumnExpression other)
        {
            return other != null && (this as object) == (other as object) || (this.Alias == other.Alias && this.Name == other.Name);
        }
    }

    internal class DbColumnDeclaration
    {
        public string Name { get; }

        public Expression Expression { get; }

        public QueryType QueryType { get; }

        public DbColumnDeclaration(string name, Expression expression, QueryType queryType)
        {
            this.Name = name ?? throw new ArgumentNullException("name");
            this.Expression = expression ?? throw new ArgumentNullException("expression");
            this.QueryType = queryType ?? throw new ArgumentNullException("queryType");
        }
    }

    internal class DbOrderExpression
    {
        public OrderType OrderType { get; }

        public Expression Expression { get; }

        public DbOrderExpression(OrderType orderType, Expression expression)
        {
            this.OrderType = orderType;
            this.Expression = expression;
        }
    }

    internal class DbSelectExpression : DbAliasedExpression
    {
        public ReadOnlyCollection<DbColumnDeclaration> Columns { get; }

        public Expression From { get; }

        public Expression Where { get; }

        public ReadOnlyCollection<DbOrderExpression> OrderBy { get; }

        public ReadOnlyCollection<Expression> GroupBy { get; }

        public bool IsDistinct { get; }

        public Expression Skip { get; }

        public Expression Take { get; }

        public bool IsReverse { get; }

        public string QueryText
        {
            get { return SqlFormatter.Format(this, true); }
        }

        public DbSelectExpression(TableAlias alias, IEnumerable<DbColumnDeclaration> columns, Expression from, Expression where) : this(alias, columns, from, where, null, null)
        {

        }

        public DbSelectExpression(TableAlias alias, IEnumerable<DbColumnDeclaration> columns, Expression from, Expression where, IEnumerable<DbOrderExpression> orderBy, IEnumerable<Expression> groupBy) : this(alias, columns, from, where, orderBy, groupBy, false, null, null, false)
        {

        }

        public DbSelectExpression(TableAlias alias, IEnumerable<DbColumnDeclaration> columns, Expression from, Expression where, IEnumerable<DbOrderExpression> orderBy, IEnumerable<Expression> groupBy, bool isDistinct, Expression skip, Expression take, bool reverse) : base(DbExpressionType.Select, typeof(void), alias)
        {
            this.Columns = columns.ToReadOnly();
            this.IsDistinct = isDistinct;
            this.From = from;
            this.Where = where;
            this.OrderBy = orderBy.ToReadOnly();
            this.GroupBy = groupBy.ToReadOnly();
            this.Take = take;
            this.Skip = skip;
            this.IsReverse = reverse;
        }
    }

    internal class DbJoinExpression : DbExpression
    {
        public JoinType JoinType { get; }

        public Expression Left { get; }

        public Expression Right { get; }

        public new Expression Condition { get; }

        public DbJoinExpression(JoinType joinType, Expression left, Expression right, Expression condition) : base(DbExpressionType.Join, typeof(void))
        {
            this.JoinType = joinType;
            this.Left = left;
            this.Right = right;
            this.Condition = condition;
        }
    }

    internal class DbOuterJoinedExpression : DbExpression
    {
        public Expression Test { get; }

        public Expression Expression { get; }

        public DbOuterJoinedExpression(Expression test, Expression expression) : base(DbExpressionType.OuterJoined, expression.Type)
        {
            this.Test = test;
            this.Expression = expression;
        }
    }

    internal class DbScalarExpression : SubqueryExpression
    {
        public DbScalarExpression(Type type, DbSelectExpression select) : base(DbExpressionType.Scalar, type, select)
        {

        }
    }

    internal class DbExistsExpression : SubqueryExpression
    {
        public DbExistsExpression(DbSelectExpression select) : base(DbExpressionType.Exists, typeof(bool), select)
        {

        }
    }

    internal class DbInExpression : SubqueryExpression
    {
        public Expression Expression { get; }

        public ReadOnlyCollection<Expression> Values { get; }

        public DbInExpression(Expression expression, DbSelectExpression select) : base(DbExpressionType.In, typeof(bool), select)
        {
            this.Expression = expression;
        }

        public DbInExpression(Expression expression, IEnumerable<Expression> values) : base(DbExpressionType.In, typeof(bool), null)
        {
            this.Expression = expression;
            this.Values = values.ToReadOnly();
        }
    }

    internal class DbAggregateExpression : DbExpression
    {
        public string AggregateName { get; }

        public Expression Argument { get; }

        public bool IsDistinct { get; }


        public DbAggregateExpression(Type type, string aggregateName, Expression argument, bool isDistinct) : base(DbExpressionType.Aggregate, type)
        {
            this.AggregateName = aggregateName;
            this.Argument = argument;
            this.IsDistinct = isDistinct;
        }
    }

    internal class DbAggregateSubqueryExpression : DbExpression
    {
        public TableAlias GroupByAlias { get; }

        public Expression AggregateInGroupSelect { get; }

        public DbScalarExpression AggregateAsSubquery { get; }

        public DbAggregateSubqueryExpression(TableAlias groupByAlias, Expression aggregateInGroupSelect, DbScalarExpression aggregateAsSubquery) : base(DbExpressionType.AggregateSubquery, aggregateAsSubquery.Type)
        {
            this.AggregateInGroupSelect = aggregateInGroupSelect;
            this.GroupByAlias = groupByAlias;
            this.AggregateAsSubquery = aggregateAsSubquery;
        }
    }

    internal class DbIsNullExpression : DbExpression
    {
        public Expression Expression { get; }

        public DbIsNullExpression(Expression expression) : base(DbExpressionType.IsNull, typeof(bool))
        {
            this.Expression = expression;
        }
    }

    internal class DbBetweenExpression : DbExpression
    {
        public Expression Expression { get; }

        public Expression Lower { get; }

        public Expression Upper { get; }

        public DbBetweenExpression(Expression expression, Expression lower, Expression upper) : base(DbExpressionType.Between, expression.Type)
        {
            this.Expression = expression;
            this.Lower = lower;
            this.Upper = upper;
        }
    }

    internal class DbRowNumberExpression : DbExpression
    {
        public ReadOnlyCollection<DbOrderExpression> OrderBy { get; }

        public DbRowNumberExpression(IEnumerable<DbOrderExpression> orderBy) : base(DbExpressionType.RowCount, typeof(int))
        {
            this.OrderBy = orderBy.ToReadOnly();
        }
    }

    internal class DbNamedValueExpression : DbExpression
    {
        public string Name { get; }

        public QueryType QueryType { get; }

        public Expression Value { get; }

        public DbNamedValueExpression(string name, QueryType queryType, Expression value) : base(DbExpressionType.NamedValue, value.Type)
        {
            this.Name = name ?? throw new ArgumentNullException("name");
            this.QueryType = queryType;
            this.Value = value ?? throw new ArgumentNullException("value");
        }
    }

    internal class DbProjectionExpression : DbExpression
    {
        public DbSelectExpression Select { get; }

        public Expression Projector { get; }

        public LambdaExpression Aggregator { get; }

        public bool IsSingleton
        {
            get { return this.Aggregator != null && this.Aggregator.Body.Type == Projector.Type; }
        }

        public string QueryText
        {
            get { return SqlFormatter.Format(Select, true); }
        }

        public DbProjectionExpression(DbSelectExpression source, Expression projector) : this(source, projector, null)
        {

        }

        public DbProjectionExpression(DbSelectExpression source, Expression projector, LambdaExpression aggregator) : base(DbExpressionType.Projection, aggregator != null ? aggregator.Body.Type : typeof(IEnumerable<>).MakeGenericType(projector.Type))
        {
            this.Select = source;
            this.Projector = projector;
            this.Aggregator = aggregator;
        }

        public override string ToString()
        {
            return DbExpressionWriter.WriteToString(this);
        }
    }

    internal class DbClientJoinExpression : DbExpression
    {
        public ReadOnlyCollection<Expression> OuterKey { get; }

        public ReadOnlyCollection<Expression> InnerKey { get; }

        public DbProjectionExpression Projection { get; }

        public DbClientJoinExpression(DbProjectionExpression projection, IEnumerable<Expression> outerKey, IEnumerable<Expression> innerKey) : base(DbExpressionType.ClientJoin, projection.Type)
        {
            this.OuterKey = outerKey.ToReadOnly();
            this.InnerKey = innerKey.ToReadOnly();
            this.Projection = projection;
        }
    }

    internal class DbBatchExpression : Expression
    {
        public Expression Input { get; }

        public LambdaExpression Operation { get; }

        public Expression BatchSize { get; }

        public Expression Stream { get; }

        public override Type Type { get; }

        public override ExpressionType NodeType { get; } = (ExpressionType)DbExpressionType.Batch;

        public DbBatchExpression(Expression input, LambdaExpression operation, Expression batchSize, Expression stream)
        {
            this.Input = input;
            this.Operation = operation;
            this.BatchSize = batchSize;
            this.Stream = stream;
            this.Type = typeof(IEnumerable<>).MakeGenericType(operation.Body.Type);
        }
    }

    internal class DbFunctionExpression : DbExpression
    {
        public string Name { get; }

        public ReadOnlyCollection<Expression> Arguments { get; }

        public DbFunctionExpression(Type type, string name, IEnumerable<Expression> arguments) : base(DbExpressionType.Function, type)
        {
            this.Name = name;
            this.Arguments = arguments.ToReadOnly();
        }
    }

    internal class DbColumnAssignment
    {
        public DbColumnExpression Column { get; }

        public Expression Expression { get; }

        public DbColumnAssignment(DbColumnExpression column, Expression expression)
        {
            this.Column = column;
            this.Expression = expression;
        }
    }

    internal class DbInsertCommand : DbCommandExpression
    {
        public DbTableExpression Table { get; }

        public ReadOnlyCollection<DbColumnAssignment> Assignments { get; }

        public DbInsertCommand(DbTableExpression table, IEnumerable<DbColumnAssignment> assignments) : base(DbExpressionType.Insert, typeof(int))
        {
            this.Table = table;
            this.Assignments = assignments.ToReadOnly();
        }
    }

    internal class DbUpdateCommand : DbCommandExpression
    {
        public DbTableExpression Table { get; }

        public Expression Where { get; }

        public ReadOnlyCollection<DbColumnAssignment> Assignments { get; }

        public DbUpdateCommand(DbTableExpression table, Expression where, IEnumerable<DbColumnAssignment> assignments) : base(DbExpressionType.Update, typeof(int))
        {
            this.Table = table;
            this.Where = where;
            this.Assignments = assignments.ToReadOnly();
        }
    }

    internal class DbDeleteCommand : DbCommandExpression
    {
        public DbTableExpression Table { get; }

        public Expression Where { get; }

        public DbDeleteCommand(DbTableExpression table, Expression where) : base(DbExpressionType.Delete, typeof(int))
        {
            this.Table = table;
            this.Where = where;
        }
    }

    internal class DbIFCommand : DbCommandExpression
    {
        public Expression Check { get; }

        public Expression IfTrue { get; }

        public Expression IfFalse { get; }

        public DbIFCommand(Expression check, Expression ifTrue, Expression ifFalse) : base(DbExpressionType.If, ifTrue.Type)
        {
            this.Check = check;
            this.IfTrue = ifTrue;
            this.IfFalse = ifFalse;
        }
    }

    internal class DbBlockCommand : DbCommandExpression
    {
        public ReadOnlyCollection<Expression> Commands { get; }

        public DbBlockCommand(params Expression[] commands) : this((IList<Expression>)commands)
        {

        }

        public DbBlockCommand(IList<Expression> commands) : base(DbExpressionType.Block, commands[commands.Count - 1].Type)
        {
            this.Commands = commands.ToReadOnly();
        }
    }

    internal class DbDeclarationCommand : DbCommandExpression
    {
        public ReadOnlyCollection<DbVariableDeclaration> Variables { get; }

        public DbSelectExpression Source { get; }

        public DbDeclarationCommand(IEnumerable<DbVariableDeclaration> variables, DbSelectExpression source) : base(DbExpressionType.Declaration, typeof(void))
        {
            this.Variables = variables.ToReadOnly();
            this.Source = source;
        }
    }

    internal class DbVariableDeclaration
    {
        public string Name { get; }

        public QueryType QueryType { get; }

        public Expression Expression { get; }

        public DbVariableDeclaration(string name, QueryType type, Expression expression)
        {
            this.Name = name;
            this.QueryType = type;
            this.Expression = expression;
        }
    }

    internal class DbVariableExpression : Expression
    {
        public string Name { get; }

        public QueryType QueryType { get; }

        public override Type Type { get; }

        public override ExpressionType NodeType { get; } = (ExpressionType)DbExpressionType.Variable;

        public DbVariableExpression(string name, Type type, QueryType queryType)
        {
            this.Name = name;
            this.QueryType = queryType;
            this.Type = type;
        }
    }
}