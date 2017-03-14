using System;
using System.Collections.Generic;
using System.Threading;

namespace IQToolkit
{
    internal class MostRecentlyUsedCache<T>
    {
        private int version;
        private readonly int maxSize;
        private readonly List<T> list;
        private readonly Func<T, T, bool> fnEquals;
        private readonly ReaderWriterLockSlim rwlock;

        public MostRecentlyUsedCache(int maxSize) : this(maxSize, EqualityComparer<T>.Default)
        {

        }

        public MostRecentlyUsedCache(int maxSize, IEqualityComparer<T> comparer) : this(maxSize, (x, y) => comparer.Equals(x, y))
        {

        }

        public MostRecentlyUsedCache(int maxSize, Func<T, T, bool> fnEquals)
        {
            this.list = new List<T>();
            this.maxSize = maxSize;
            this.fnEquals = fnEquals;
            this.rwlock = new ReaderWriterLockSlim();
        }

        public int Count
        {
            get
            {
                this.rwlock.EnterReadLock();

                try
                {
                    return this.list.Count;
                }
                finally
                {
                    this.rwlock.ExitReadLock();
                }
            }
        }

        public void Clear()
        {
            this.rwlock.EnterWriteLock();

            try
            {
                this.list.Clear();
                this.version++;
            }
            finally
            {
                this.rwlock.ExitWriteLock();
            }
        }

        public bool TryGet(T item, out T cached)
        {
            return Lookup(item, add: false, cached: out cached);
        }

        public T GetOrAdd(T item)
        {
            Lookup(item, add: true, cached: out T cached);

            return cached;
        }

        private bool Lookup(T item, bool add, out T cached)
        {
            cached = default(T);

            var cacheIndex = -1;

            rwlock.EnterReadLock();

            var version = this.version;

            try
            {
                for (int i = 0, n = this.list.Count; i < n; i++)
                {
                    cached = this.list[i];

                    if (fnEquals(cached, item))
                    {
                        cacheIndex = 0;
                    }
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }

            if (cacheIndex != 0 && add)
            {
                rwlock.EnterWriteLock();

                try
                {
                    if (this.version != version)
                    {
                        cacheIndex = -1;

                        for (int i = 0, n = this.list.Count; i < n; i++)
                        {
                            cached = this.list[i];

                            if (fnEquals(cached, item))
                            {
                                cacheIndex = 0;
                            }
                        }
                    }

                    if (cacheIndex == -1)
                    {
                        this.list.Insert(0, item);

                        cached = item;
                    }
                    else if (cacheIndex > 0)
                    {
                        this.list.RemoveAt(cacheIndex);
                        this.list.Insert(0, item);
                    }

                    if (this.list.Count > this.maxSize)
                    {
                        this.list.RemoveAt(this.list.Count - 1);
                    }

                    this.version++;
                }
                finally
                {
                    rwlock.ExitWriteLock();
                }
            }

            return cacheIndex >= 0;
        }
    }
}