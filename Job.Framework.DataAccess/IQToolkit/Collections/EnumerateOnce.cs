﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    internal class EnumerateOnce<T> : IEnumerableAsync<T>, IEnumerable<T>, IEnumerable
    {
        IEnumerable<T> enumerable;

        public EnumerateOnce(IEnumerable<T> enumerable)
        {
            this.enumerable = enumerable;
        }

        public T Current
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public IEnumerator<T> GetEnumerator()
        {
            var en = Interlocked.Exchange(ref enumerable, null);

            if (en != null)
            {
                return en.GetEnumerator();
            }

            throw new Exception("Enumerated more than once.");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public Task<IEnumeratorAsync<T>> GetEnumeratorAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(this.GetEnumerator().ToAsync());
        }
    }
}