using IQToolkit.Data.Common;
using System;
using System.Data.Common;

namespace IQToolkit.Data.MySql
{
    internal class MySqlQueryProvider : DbEntityProvider
    {
        public MySqlQueryProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) : base(connection, MySqlLanguage.Default, mapping, policy)
        {

        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new MySqlQueryProvider(connection, mapping, policy);
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        private class Executor : DbQueryExecutor
        {
            private readonly MySqlQueryProvider provider;

            public Executor(MySqlQueryProvider provider) : base(provider)
            {
                this.provider = provider;
            }

            public override bool BufferResultRows
            {
                get { return true; }
            }

            protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                var p = command.CreateParameter();

                p.ParameterName = "?" + parameter.Name;
                p.Value = value ?? DBNull.Value;

                command.Parameters.Add(p);
            }
        }
    }
}