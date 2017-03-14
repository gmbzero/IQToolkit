using IQToolkit.Data.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit.Data
{
    internal partial class DbEntityProvider
    {
        public partial class DbQueryExecutor
        {
            public class DbQueryCommandBatchExecutable : IEnumerableAsync<int>
            {
                private readonly DbQueryExecutor executor;
                private readonly QueryCommand query;
                private readonly object[][] paramSets;

                public DbQueryCommandBatchExecutable(DbQueryExecutor executor, QueryCommand query, object[][] paramSets)
                {
                    this.executor = executor;
                    this.query = query;
                    this.paramSets = paramSets;
                }

                public Task<IEnumeratorAsync<int>> GetEnumeratorAsync(CancellationToken cancellationToken)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    return Task.FromResult<IEnumeratorAsync<int>>(new Enumerator(this));
                }

                public IEnumerator<int> GetEnumerator()
                {
                    return new Enumerator(this);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return this.GetEnumerator();
                }

                public class Enumerator : IEnumeratorAsync<int>, IEnumerator<int>, IDisposable
                {
                    private int paramSet;
                    private int current;
                    private bool closed;
                    private readonly DbCommand command;
                    private readonly DbQueryCommandBatchExecutable parent;

                    public Enumerator(DbQueryCommandBatchExecutable parent)
                    {
                        this.parent = parent;
                        this.parent.executor.LogCommand(this.parent.query);
                        this.parent.executor.StartUsingConnection();
                        this.command = this.parent.executor.GetCommand(this.parent.query);
                        this.command.Prepare();
                        this.paramSet = -1;
                    }

                    public int Current
                    {
                        get { return this.current; }
                    }

                    object IEnumerator.Current
                    {
                        get { return this.Current; }
                    }

                    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (this.paramSet < this.parent.paramSets.Length - 1)
                        {
                            this.paramSet++;
                            this.parent.executor.LogParameters(this.parent.query, this.parent.paramSets[this.paramSet]);
                            this.parent.executor.SetParameterValues(this.parent.query, this.command, this.parent.paramSets[this.paramSet]);
                            this.current = await this.command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                            return true;
                        }
                        else
                        {
                            this.Dispose();

                            return false;
                        }
                    }

                    public bool MoveNext()
                    {
                        if (this.paramSet < this.parent.paramSets.Length - 1)
                        {
                            this.paramSet++;
                            this.parent.executor.LogParameters(this.parent.query, this.parent.paramSets[this.paramSet]);
                            this.parent.executor.SetParameterValues(this.parent.query, this.command, this.parent.paramSets[this.paramSet]);
                            this.current = this.command.ExecuteNonQuery();

                            return true;
                        }
                        else
                        {
                            this.Dispose();

                            return false;
                        }
                    }

                    public void Dispose()
                    {
                        if (this.closed == false)
                        {
                            this.closed = true;
                            this.parent.executor.StopUsingConnection();
                        }
                    }

                    public void Reset()
                    {
                        throw new NotSupportedException();
                    }
                }
            }
        }
    }
}