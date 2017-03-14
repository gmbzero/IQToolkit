using System.Collections.Generic;
using System.Linq;

namespace IQToolkit
{
    internal struct DeferredValue<T> : IDeferLoadable
    {
        private T value;
        private bool loaded;
        private IEnumerable<T> source;

        public bool IsLoaded
        {
            get { return this.loaded; }
        }

        public bool IsAssigned
        {
            get { return this.loaded && this.source == null; }
        }

        public T Value
        {
            get
            {
                this.Check();

                return this.value;
            }

            set
            {
                this.value = value;

                this.loaded = true;
                this.source = null;
            }
        }

        public DeferredValue(T value)
        {
            this.value = value;

            this.source = null;
            this.loaded = true;
        }

        public DeferredValue(IEnumerable<T> source)
        {
            this.value = default(T);

            this.source = source;
            this.loaded = false;
        }

        public void Load()
        {
            if (this.source != null)
            {
                this.value = this.source.SingleOrDefault();
                this.loaded = true;
            }
        }

        private void Check()
        {
            if (!this.IsLoaded)
            {
                this.Load();
            }
        }
    }
}