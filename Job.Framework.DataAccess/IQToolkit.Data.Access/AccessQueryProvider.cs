using IQToolkit.Data.Common;
using System;
using System.Data.Common;

namespace IQToolkit.Data.Access
{
    internal class AccessQueryProvider : DbEntityProvider
    {
        public AccessQueryProvider(DbConnection connection, QueryMapping mapping, QueryPolicy policy) : base(connection, AccessLanguage.Default, mapping, policy)
        {

        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new AccessQueryProvider(connection, mapping, policy);
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        public class Executor : DbQueryExecutor
        {
            private readonly AccessQueryProvider provider;

            public Executor(AccessQueryProvider provider) : base(provider)
            {
                this.provider = provider;
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