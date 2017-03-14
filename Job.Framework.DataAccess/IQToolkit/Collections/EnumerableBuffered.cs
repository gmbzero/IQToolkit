using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    public sealed class EnumerableBuffered<T> : IEnumerableAsync<T>
    {
        private readonly IEnumerable<T> enumerable;
        private IEnumerable<T> buffer;

        public EnumerableBuffered(IEnumerable<T> enumerable)
        {
            this.enumerable = enumerable;
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (this.buffer == null)
            {
                this.buffer = this.enumerable.ToList();
            }

            return this.buffer.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public async Task<IEnumeratorAsync<T>> GetEnumeratorAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (this.buffer == null)
            {
                this.buffer = await this.enumerable.ToListAsync(cancellationToken).ConfigureAwait(false);
            }

            return this.buffer.GetEnumerator().ToAsync();
        }
    }
}