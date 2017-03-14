using IQToolkit.Data.Common;
using System;
using System.Data.Common;

namespace IQToolkit.Data.SqlServerCe
{
    internal class SqlCeQueryProvider : DbEntityProvider
    {
        public SqlCeQueryProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) : base(connection, SqlCeLanguage.Default, mapping, policy)
        {

        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new SqlCeQueryProvider(connection, mapping, policy);
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        private class Executor : DbQueryExecutor
        {
            private readonly SqlCeQueryProvider provider;

            public Executor(SqlCeQueryProvider provider) : base(provider)
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