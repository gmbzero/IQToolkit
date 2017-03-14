using System;
using System.Collections;
using System.Collections.Generic;

namespace IQToolkit
{
    internal interface IDeferLoadable
    {
        bool IsLoaded { get; }
        void Load();
    }

    internal class DeferredList<T> : IList<T>, IDeferLoadable
    {
        private List<T> values;
        private readonly IEnumerable<T> source;

        public bool IsLoaded
        {
            get { return this.values != null; }
        }

        public DeferredList(IEnumerable<T> source)
        {
            this.source = source;
        }

        public void Load()
        {
            this.values = new List<T>(this.source);
        }

        private void Check()
        {
            if (this.IsLoaded == false)
            {
                this.Load();
            }
        }

        #region IList<T> Members

        public int IndexOf(T item)
        {
            this.Check();

            return this.values.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            this.Check();

            this.values.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            this.Check();

            this.values.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                this.Check();

                return this.values[index];
            }
            set
            {
                this.Check();

                this.values[index] = value;
            }
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            this.Check();

            this.values.Add(item);
        }

        public void Clear()
        {
            this.Check();

            this.values.Clear();
        }

        public bool Contains(T item)
        {
            this.Check();

            return this.values.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            this.Check();

            this.values.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { this.Check(); return this.values.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(T item)
        {
            this.Check();

            return this.values.Remove(item);
        }

        #endregion

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            this.Check();

            return this.values.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion

        #region IList Members

        public int Add(object value)
        {
            this.Check();

            return (this.values as IList).Add(value);
        }

        public bool Contains(object value)
        {
            this.Check();

            return (this.values as IList).Contains(value);
        }

        public int IndexOf(object value)
        {
            this.Check();

            return (this.values as IList).IndexOf(value);
        }

        public void Insert(int index, object value)
        {
            this.Check();

            (this.values as IList).Insert(index, value);
        }

        public bool IsFixedSize
        {
            get { return false; }
        }

        public void Remove(object value)
        {
            this.Check();

            (this.values as IList).Remove(value);
        }


        #endregion

        #region ICollection Members

        public void CopyTo(Array array, int index)
        {
            this.Check();

            (this.values as IList).CopyTo(array, index);
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public object SyncRoot
        {
            get { return null; }
        }

        #endregion
    }
}