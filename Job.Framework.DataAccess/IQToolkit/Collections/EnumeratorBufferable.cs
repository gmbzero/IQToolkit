using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace IQToolkit
{
    public interface IBufferableAsync
    {
        void Buffer();
        Task BufferAsync(CancellationToken cancellationToken);
    }

    public sealed class EnumeratorBufferable<T> : IEnumeratorAsync<T>, IBufferableAsync
    {
        private bool buffered;
        private IEnumerator<T> enumerator;

        public T Current
        {
            get { return this.enumerator.Current; }
        }

        object IEnumerator.Current
        {
            get { return this.Current; }
        }

        public EnumeratorBufferable(IEnumerator<T> enumerator)
        {
            this.enumerator = enumerator;
        }

        public void Buffer()
        {
            if (this.buffered == false)
            {
                this.buffered = true;

                using (var original = this.enumerator)
                {
                    this.enumerator = this.enumerator.ToList().GetEnumerator();
                }
            }
        }

        public async Task BufferAsync(CancellationToken cancellationToken)
        {
            if (this.buffered == false)
            {
                this.buffered = true;

                using (var original = this.enumerator)
                {
                    var list = await this.enumerator.ToAsync().ToListAsync(cancellationToken).ConfigureAwait(false);

                    if (list != null)
                    {
                        this.enumerator = list.GetEnumerator().ToAsync();
                    }
                }
            }
        }

        public bool MoveNext()
        {
            return this.enumerator.MoveNext();
        }

        public Task<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            var ae = this.enumerator as IEnumeratorAsync<T>;

            if (ae == null)
            {
                this.enumerator = ae = this.enumerator.ToAsync();
            }

            return ae.MoveNextAsync(cancellationToken);
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