/** Fixed-capacity FIFO: once full, each push overwrites the oldest entry. O(1) push. */
export class RingBuffer<T> {
  private readonly slots: Array<T | undefined>;
  private start = 0;
  private count = 0;

  constructor(readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError('capacity must be a positive integer');
    this.slots = new Array<T | undefined>(capacity);
  }

  push(item: T): void {
    const end = (this.start + this.count) % this.capacity;
    this.slots[end] = item;
    if (this.count < this.capacity) {
      this.count += 1;
    } else {
      this.start = (this.start + 1) % this.capacity;
    }
  }

  /** Items from oldest to newest. */
  toArray(): T[] {
    const items: T[] = [];
    for (let i = 0; i < this.count; i += 1) {
      items.push(this.slots[(this.start + i) % this.capacity] as T);
    }
    return items;
  }

  get length(): number {
    return this.count;
  }
}
