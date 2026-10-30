/**
 * Token bucket: holds up to `capacity` tokens and refills continuously at `refillPerSecond`.
 * Each inbound frame costs one token, so a client can burst `capacity` frames and then sustain
 * `refillPerSecond` frames per second.
 */
export class TokenBucket {
  private tokens: number;
  private updatedAt: number;

  constructor(
    readonly capacity: number,
    readonly refillPerSecond: number,
    private readonly clock: () => number = () => performance.now(),
  ) {
    if (capacity <= 0 || refillPerSecond <= 0) throw new RangeError('capacity and refill rate must be positive');
    this.tokens = capacity;
    this.updatedAt = clock();
  }

  tryRemove(cost = 1): boolean {
    this.refill();
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }

  /** Milliseconds until `cost` tokens are available (0 if they already are). */
  msUntilAvailable(cost = 1): number {
    this.refill();
    const missing = cost - this.tokens;
    return missing <= 0 ? 0 : Math.ceil((missing / this.refillPerSecond) * 1000);
  }

  get available(): number {
    this.refill();
    return this.tokens;
  }

  private refill(): void {
    const now = this.clock();
    const elapsedSeconds = Math.max(0, now - this.updatedAt) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsedSeconds * this.refillPerSecond);
    this.updatedAt = now;
  }
}
