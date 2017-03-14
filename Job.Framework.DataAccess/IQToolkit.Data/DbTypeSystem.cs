
using IQToolkit.Data.Access;
using IQToolkit.Data.Common;
using IQToolkit.Data.Mapping;
using IQToolkit.Data.MySql;
using IQToolkit.Data.Oracle;
using IQToolkit.Data.SQLite;
using IQToolkit.Data.SqlServer;
using IQToolkit.Data.SqlServerCe;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data
{
    internal enum SqlDbType
    {
        BigInt = 0,
        Binary = 1,
        Bit = 2,
        Char = 3,
        DateTime = 4,
        Decimal = 5,
        Float = 6,
        Image = 7,
        Int = 8,
        Money = 9,
        NChar = 10,
        NText = 11,
        NVarChar = 12,
        Real = 13,
        UniqueIdentifier = 14,
        SmallDateTime = 15,
        SmallInt = 16,
        SmallMoney = 17,
        Text = 18,
        Timestamp = 19,
        TinyInt = 20,
        VarBinary = 21,
        VarChar = 22,
        Variant = 23,
        Xml = 25,
        Udt = 29,
        Structured = 30,
        Date = 31,
        Time = 32,
        DateTime2 = 33,
        DateTimeOffset = 34
    }

    internal class DbTypeSystem : QueryTypeSystem
    {
        internal static QueryMapping GetMapping(Type contextType, PropertyInfo property)
        {
            if (property.GetCustomAttributes(typeof(BaseAttribute)).Count() > 0)
            {
                return new AttributeMapping(contextType);
            }

            return new ImplicitMapping();
        }

        internal static DbEntityProvider GetProvider(DbConnection dbConnection, QueryMapping mapping, QueryPolicy policy)
        {
            var connectionType = dbConnection.GetType();

            switch (connectionType.Name)
            {
                case "OleDbConnection":
                    {
                        return new AccessQueryProvider(dbConnection, mapping, policy);
                    }
                case "MySqlConnection":
                    {
                        return new MySqlQueryProvider(dbConnection, mapping, policy);
                    }
                case "OracleConnection":
                    {
                        return new OracleQueryProvider(dbConnection, mapping, policy);
                    }
                case "SQLiteConnection":
                    {
                        return new SQLiteQueryProvider(dbConnection, mapping, policy);
                    }
                case "SqlConnection":
                    {
                        return new TSqlQueryProvider(dbConnection, mapping, policy);
                    }
                case "SqlCeConnection":
                    {
                        return new SqlCeQueryProvider(dbConnection, mapping, policy);
                    }
                default:
                    {
                        throw new InvalidOperationException($"不支持 { connectionType.Name } 的数据库访问");
                    }
            }
        }


        public override QueryType Parse(string typeDeclaration)
        {
            var args = null as string[];
            var typeName = null as string;
            var remainder = null as string;
            var openParen = typeDeclaration.IndexOf('(');

            if (openParen >= 0)
            {
                typeName = typeDeclaration.Substring(0, openParen).Trim();

                var closeParen = typeDeclaration.IndexOf(')', openParen);

                if (closeParen < openParen)
                {
                    closeParen = typeDeclaration.Length;
                }

                var argstr = typeDeclaration.Substring(openParen + 1, closeParen - (openParen + 1));
                args = argstr.Split(',');
                remainder = typeDeclaration.Substring(closeParen + 1);
            }
            else
            {
                var space = typeDeclaration.IndexOf(' ');

                if (space >= 0)
                {
                    typeName = typeDeclaration.Substring(0, space);
                    remainder = typeDeclaration.Substring(space + 1).Trim();
                }
                else
                {
                    typeName = typeDeclaration;
                }
            }

            var isNotNull = (remainder != null) ? remainder.ToUpper().Contains("NOT NULL") : false;

            return this.GetQueryType(typeName, args, isNotNull);
        }

        public virtual QueryType GetQueryType(string typeName, string[] args, bool isNotNull)
        {
            if (string.Compare(typeName, "rowversion", StringComparison.CurrentCultureIgnoreCase) == 0)
            {
                typeName = "Timestamp";
            }

            if (string.Compare(typeName, "numeric", StringComparison.CurrentCultureIgnoreCase) == 0)
            {
                typeName = "Decimal";
            }

            if (string.Compare(typeName, "sql_variant", StringComparison.CurrentCultureIgnoreCase) == 0)
            {
                typeName = "Variant";
            }

            var length = 0;
            var precision = (short)0;
            var scale = (short)0;

            var dbType = this.GetSqlType(typeName);

            switch (dbType)
            {
                case SqlDbType.Binary:
                case SqlDbType.Char:
                case SqlDbType.Image:
                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    {
                        if (args == null || args.Length < 1)
                        {
                            length = 80;
                        }
                        else if (string.Compare(args[0], "max", true) == 0)
                        {
                            length = Int32.MaxValue;
                        }
                        else
                        {
                            length = Int32.Parse(args[0]);
                        }

                        break;
                    }
                case SqlDbType.Money:
                    {
                        if (args == null || args.Length < 1)
                        {
                            precision = 29;
                        }
                        else
                        {
                            precision = Int16.Parse(args[0]);
                        }
                        if (args == null || args.Length < 2)
                        {
                            scale = 4;
                        }
                        else
                        {
                            scale = Int16.Parse(args[1]);
                        }

                        break;
                    }
                case SqlDbType.Decimal:
                    {
                        if (args == null || args.Length < 1)
                        {
                            precision = 29;
                        }
                        else
                        {
                            precision = Int16.Parse(args[0]);
                        }
                        if (args == null || args.Length < 2)
                        {
                            scale = 0;
                        }
                        else
                        {
                            scale = Int16.Parse(args[1]);
                        }

                        break;
                    }
                case SqlDbType.Float:
                case SqlDbType.Real:
                    {
                        if (args == null || args.Length < 1)
                        {
                            precision = 29;
                        }
                        else
                        {
                            precision = Int16.Parse(args[0]);
                        }

                        break;
                    }
            }

            return NewType(dbType, isNotNull, length, precision, scale);
        }

        public virtual QueryType NewType(SqlDbType type, bool isNotNull, int length, short precision, short scale)
        {
            return new DbQueryType(type, isNotNull, length, precision, scale);
        }

        public virtual SqlDbType GetSqlType(string typeName)
        {
            return (SqlDbType)Enum.Parse(typeof(SqlDbType), typeName, true);
        }

        public virtual int StringDefaultSize
        {
            get { return Int32.MaxValue; }
        }

        public virtual int BinaryDefaultSize
        {
            get { return Int32.MaxValue; }
        }

        public override QueryType GetColumnType(Type type)
        {
            var isNotNull = type.GetTypeInfo().IsValueType && !TypeHelper.IsNullableType(type);

            type = TypeHelper.GetNonNullableType(type);

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                    return NewType(SqlDbType.Bit, isNotNull, 0, 0, 0);
                case TypeCode.SByte:
                case TypeCode.Byte:
                    return NewType(SqlDbType.TinyInt, isNotNull, 0, 0, 0);
                case TypeCode.Int16:
                case TypeCode.UInt16:
                    return NewType(SqlDbType.SmallInt, isNotNull, 0, 0, 0);
                case TypeCode.Int32:
                case TypeCode.UInt32:
                    return NewType(SqlDbType.Int, isNotNull, 0, 0, 0);
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return NewType(SqlDbType.BigInt, isNotNull, 0, 0, 0);
                case TypeCode.Single:
                case TypeCode.Double:
                    return NewType(SqlDbType.Float, isNotNull, 0, 0, 0);
                case TypeCode.String:
                    return NewType(SqlDbType.NVarChar, isNotNull, this.StringDefaultSize, 0, 0);
                case TypeCode.Char:
                    return NewType(SqlDbType.NChar, isNotNull, 1, 0, 0);
                case TypeCode.DateTime:
                    return NewType(SqlDbType.DateTime, isNotNull, 0, 0, 0);
                case TypeCode.Decimal:
                    return NewType(SqlDbType.Decimal, isNotNull, 0, 29, 4);
                default:
                    {
                        if (type == typeof(byte[])) return NewType(SqlDbType.VarBinary, isNotNull, this.BinaryDefaultSize, 0, 0);
                        else if (type == typeof(Guid)) return NewType(SqlDbType.UniqueIdentifier, isNotNull, 0, 0, 0);
                        else if (type == typeof(DateTimeOffset)) return NewType(SqlDbType.DateTimeOffset, isNotNull, 0, 0, 0);
                        else if (type == typeof(TimeSpan)) return NewType(SqlDbType.Time, isNotNull, 0, 0, 0);

                        return null;
                    }
            }
        }

        public static DbType GetDbType(SqlDbType dbType)
        {
            switch (dbType)
            {
                case SqlDbType.BigInt:
                    return DbType.Int64;
                case SqlDbType.Binary:
                    return DbType.Binary;
                case SqlDbType.Bit:
                    return DbType.Boolean;
                case SqlDbType.Char:
                    return DbType.AnsiStringFixedLength;
                case SqlDbType.Date:
                    return DbType.Date;
                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    return DbType.DateTime;
                case SqlDbType.DateTime2:
                    return DbType.DateTime2;
                case SqlDbType.DateTimeOffset:
                    return DbType.DateTimeOffset;
                case SqlDbType.Decimal:
                    return DbType.Decimal;
                case SqlDbType.Float:
                case SqlDbType.Real:
                    return DbType.Double;
                case SqlDbType.Image:
                    return DbType.Binary;
                case SqlDbType.Int:
                    return DbType.Int32;
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return DbType.Currency;
                case SqlDbType.NChar:
                    return DbType.StringFixedLength;
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                    return DbType.String;
                case SqlDbType.SmallInt:
                    return DbType.Int16;
                case SqlDbType.Text:
                    return DbType.AnsiString;
                case SqlDbType.Time:
                    return DbType.Time;
                case SqlDbType.Timestamp:
                    return DbType.Binary;
                case SqlDbType.TinyInt:
                    return DbType.SByte;
                case SqlDbType.Udt:
                    return DbType.Object;
                case SqlDbType.UniqueIdentifier:
                    return DbType.Guid;
                case SqlDbType.VarBinary:
                    return DbType.Binary;
                case SqlDbType.VarChar:
                    return DbType.AnsiString;
                case SqlDbType.Variant:
                    return DbType.Object;
                case SqlDbType.Xml:
                    return DbType.String;
                default:
                    {
                        throw new InvalidOperationException($"Unhandled sql type: { dbType }");
                    }
            }
        }

        public static bool IsVariableLength(SqlDbType dbType)
        {
            switch (dbType)
            {
                case SqlDbType.Image:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                case SqlDbType.Xml:
                    return true;
                default:
                    return false;
            }
        }

        public override string GetVariableDeclaration(QueryType type, bool suppressSize)
        {
            var sqlType = type as DbQueryType;
            var sb = new StringBuilder();

            sb.Append(sqlType.SqlDbType.ToString().ToUpper());

            if (sqlType.Length > 0 && !suppressSize)
            {
                if (sqlType.Length == Int32.MaxValue)
                {
                    sb.Append("(max)");
                }
                else
                {
                    sb.AppendFormat("({0})", sqlType.Length);
                }
            }
            else if (sqlType.Precision != 0)
            {
                if (sqlType.Scale != 0)
                {
                    sb.AppendFormat("({0},{1})", sqlType.Precision, sqlType.Scale);
                }
                else
                {
                    sb.AppendFormat("({0})", sqlType.Precision);
                }
            }

            return sb.ToString();
        }
    }

    internal class DbQueryType : QueryType
    {
        private readonly SqlDbType dbType;
        private readonly bool notNull;
        private readonly int length;
        private readonly short precision;
        private readonly short scale;

        public DbType DbType
        {
            get { return DbTypeSystem.GetDbType(this.dbType); }
        }

        public SqlDbType SqlDbType
        {
            get { return this.dbType; }
        }

        public override int Length
        {
            get { return this.length; }
        }

        public override bool NotNull
        {
            get { return this.notNull; }
        }

        public override short Precision
        {
            get { return this.precision; }
        }

        public override short Scale
        {
            get { return this.scale; }
        }

        public DbQueryType(SqlDbType dbType, bool notNull, int length, short precision, short scale)
        {
            this.dbType = dbType;
            this.notNull = notNull;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }
    }
}