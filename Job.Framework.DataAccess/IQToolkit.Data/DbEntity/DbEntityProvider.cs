using IQToolkit.Data.Common;
using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit.Data
{
    internal partial class DbEntityProvider : EntityProvider
    {
        private int nConnectedActions = 0;
        private bool actionOpenedConnection = false;
        private readonly DbConnection connection;
        private DbTransaction transaction;
        private IsolationLevel isolation = IsolationLevel.ReadCommitted;

        public virtual DbConnection Connection
        {
            get { return this.connection; }
        }

        public virtual DbTransaction Transaction
        {
            get { return this.transaction; }
            set
            {
                if (value != null && value.Connection != this.connection)
                {
                    throw new InvalidOperationException("Transaction does not match connection.");
                }

                this.transaction = value;
            }
        }

        public IsolationLevel Isolation
        {
            get { return this.isolation; }
            set { this.isolation = value; }
        }

        public DbEntityProvider(DbConnection connection, QueryLanguage language, QueryMapping mapping, QueryPolicy policy) : base(language, mapping, policy)
        {
            this.connection = connection ?? throw new InvalidOperationException("Connection not specified");
        }

        public virtual DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return Activator.CreateInstance(this.GetType(), new object[] { connection, mapping, policy }) as DbEntityProvider;
        }
        
        protected bool ActionOpenedConnection
        {
            get { return this.actionOpenedConnection; }
        }

        protected void StartUsingConnection()
        {
            if (this.connection.State == ConnectionState.Closed)
            {
                this.connection.Open();
                this.actionOpenedConnection = true;
            }

            this.nConnectedActions++;
        }

        protected void StopUsingConnection()
        {
            this.nConnectedActions--;

            if (this.nConnectedActions == 0 && this.actionOpenedConnection)
            {
                this.connection.Close();
                this.actionOpenedConnection = false;
            }
        }

        public override void DoConnected(Action action)
        {
            this.StartUsingConnection();

            try
            {
                action();
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override async Task DoConnectedAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
        {
            this.StartUsingConnection();

            try
            {
                await action(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override void DoTransacted(Action action)
        {
            this.StartUsingConnection();

            try
            {
                if (this.Transaction == null)
                {
                    var trans = this.Connection.BeginTransaction(this.Isolation);

                    try
                    {
                        this.Transaction = trans;

                        action();

                        trans.Commit();
                    }
                    finally
                    {
                        this.Transaction = null;

                        trans.Dispose();
                    }
                }
                else
                {
                    action();
                }
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override async Task DoTransactedAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
        {
            this.StartUsingConnection();

            try
            {
                if (this.Transaction == null)
                {
                    var trans = this.Connection.BeginTransaction(this.Isolation);

                    try
                    {
                        this.Transaction = trans;

                        await action(cancellationToken).ConfigureAwait(false);

                        trans.Commit();
                    }
                    finally
                    {
                        this.Transaction = null;

                        trans.Dispose();
                    }
                }
                else
                {
                    await action(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override int ExecuteCommand(string commandText)
        {
            if (this.Log != null)
            {
                this.Log.WriteLine(commandText);
            }

            this.StartUsingConnection();

            try
            {
                var cmd = this.Connection.CreateCommand();

                cmd.CommandText = commandText;

                return cmd.ExecuteNonQuery();
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override async Task<int> ExecuteCommandAsync(string commandText, CancellationToken cancellationToken)
        {
            if (this.Log != null)
            {
                this.Log.WriteLine(commandText);
            }

            this.StartUsingConnection();

            try
            {
                var cmd = this.Connection.CreateCommand();

                cmd.CommandText = commandText;

                return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new DbQueryExecutor(this);
        }
    }
}