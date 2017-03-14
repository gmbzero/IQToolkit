using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace IQToolkit
{
    internal class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        private readonly TKey key;
        private IEnumerable<TElement> group;

        public TKey Key
        {
            get { return this.key; }
        }

        public Grouping(TKey key, IEnumerable<TElement> group)
        {
            this.key = key;
            this.group = group;
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            if ((group is List<TElement>) == false)
            {
                group = group.ToList();
            }

            return this.group.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.group.GetEnumerator();
        }
    }
}