using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    public interface IEnumeratorAsync<out T> : IEnumerator<T>
    {
        Task<bool> MoveNextAsync(CancellationToken cancellationToken = default(CancellationToken));
    }

    public static class EnumeratorAsync
    {
        public static IEnumeratorAsync<T> ToAsync<T>(this IEnumerator<T> enumerator)
        {
            var ae = enumerator as IEnumeratorAsync<T>;

            if (ae == null)
            {
                ae = new Wrapper<T>(enumerator);
            }

            return ae;
        }

        public static async Task<List<T>> ToListAsync<T>(this IEnumeratorAsync<T> enumerator, CancellationToken cancellationToken = default(CancellationToken))
        {
            var list = new List<T>();

            while (await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
                list.Add(enumerator.Current);
            }

            return list;
        }

        public static List<T> ToList<T>(this IEnumerator<T> enumerator)
        {
            var list = new List<T>();

            while (enumerator.MoveNext())
            {
                list.Add(enumerator.Current);
            }

            return list;
        }

        private class Wrapper<T> : IEnumeratorAsync<T>
        {
            private readonly IEnumerator<T> enumerator;

            public T Current
            {
                get
                {
                    return this.enumerator.Current;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return this.enumerator.Current;
                }
            }

            public Wrapper(IEnumerator<T> enumerator)
            {
                this.enumerator = enumerator;
            }

            public bool MoveNext()
            {
                return this.enumerator.MoveNext();
            }

            public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default(CancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                return Task.FromResult(this.MoveNext());
            }

            public void Reset()
            {
                this.enumerator.Reset();
            }

            public void Dispose()
            {
                this.enumerator.Dispose();
            }
        }
    }
}