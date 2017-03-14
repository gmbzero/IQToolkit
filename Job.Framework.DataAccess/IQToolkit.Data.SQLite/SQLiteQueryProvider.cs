using IQToolkit.Data.Common;
using System;
using System.Data.Common;

namespace IQToolkit.Data.SQLite
{
    internal class SQLiteQueryProvider : DbEntityProvider
    {
        public SQLiteQueryProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) : base(connection, SQLiteLanguage.Default, mapping, policy)
        {

        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new SQLiteQueryProvider(connection, mapping, policy);
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        private class Executor : DbQueryExecutor
        {
            private readonly SQLiteQueryProvider provider;

            public Executor(SQLiteQueryProvider provider) : base(provider)
            {
                this.provider = provider;
            }

            protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                var p = command.CreateParameter();

                p.ParameterName = "@" + parameter.Name;
                p.Value = value ?? DBNull.Value;

                command.Parameters.Add(p);
            }
        }
    }
}