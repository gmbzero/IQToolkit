using System;
using System.Linq.Expressions;

namespace IQToolkit.Data.Common
{
    internal enum DbExpressionType
    {
        Table = 1000,
        ClientJoin,
        Column,
        Select,
        Projection,
        Entity,
        Join,
        Aggregate,
        Scalar,
        Exists,
        In,
        Grouping,
        AggregateSubquery,
        IsNull,
        Between,
        RowCount,
        NamedValue,
        OuterJoined,
        Insert,
        Update,
        Delete,
        Batch,
        Function,
        Block,
        If,
        Declaration,
        Variable
    }

    internal enum OrderType
    {
        Ascending,
        Descending
    }

    internal enum JoinType
    {
        CrossJoin,
        InnerJoin,
        CrossApply,
        OuterApply,
        LeftOuter,
        SingletonLeftOuter
    }

    internal static class DbExpressionTypeExtensions
    {
        public static bool IsDbExpression(this ExpressionType et)
        {
            return ((int)et) >= 1000;
        }
    }

    internal class TableAlias
    {
        public override string ToString()
        {
            return "A:" + this.GetHashCode();
        }
    }

    internal abstract class DbExpression : Expression
    {
        public override Type Type { get; }

        public override ExpressionType NodeType { get; }

        protected DbExpression(DbExpressionType eType, Type type)
        {
            this.Type = type;
            this.NodeType = (ExpressionType)eType;
        }

        public override string ToString()
        {
            return DbExpressionWriter.WriteToString(this);
        }
    }

    internal abstract class DbAliasedExpression : DbExpression
    {
        public TableAlias Alias { get; }

        protected DbAliasedExpression(DbExpressionType nodeType, Type type, TableAlias alias) : base(nodeType, type)
        {
            this.Alias = alias;
        }
    }

    internal abstract class SubqueryExpression : DbExpression
    {
        public DbSelectExpression Select { get; }

        protected SubqueryExpression(DbExpressionType eType, Type type, DbSelectExpression select) : base(eType, type)
        {
            this.Select = select;
        }
    }

    internal abstract class DbCommandExpression : DbExpression
    {
        protected DbCommandExpression(DbExpressionType eType, Type type) : base(eType, type)
        {

        }
    }
}