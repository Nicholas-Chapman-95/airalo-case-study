# Order status transitions

Analysis of status change patterns in `raw.orders`, performed during the
build of `int_orders_latest`.

---

## Background

`raw.orders` contains multiple rows per `order_id` — one per status change.
`int_orders_latest` deduplicates to one row per order, keeping the row with
the most recent `updated_at`.

During development, three `was_ever_*` boolean flags were added
(`was_ever_completed`, `was_ever_failed`, `was_ever_refunded`) to capture
whether an order had ever held a given status, regardless of its current
state. These were subsequently removed after the analysis below showed they
carried no additional information over the existing `is_*` flags.

## Findings (2,704 orders)

1. **Every order was completed at some point.**
   `was_ever_completed = true` for all 2,704 orders. No order exists that
   was only ever failed or only ever refunded without first being completed.

2. **Failed and refunded are terminal states.**
   There are zero orders where `was_ever_failed = true` and `is_failed = false`,
   and zero where `was_ever_refunded = true` and `is_refunded = false`. Once an
   order moves to `failed` or `refunded`, it stays there.

3. **The only observed transition is `completed → failed` or `completed → refunded`.**
   Orders start as `completed` and may later transition to `failed` (likely a
   payment reversal, chargeback, or fraud check) or `refunded`. There are no
   reverse transitions (e.g. `failed → completed`).

## Implications

- The `is_completed`, `is_failed`, and `is_refunded` flags on the latest row
  are sufficient — historical flags add no information.
- `int_orders_latest` can be thought of as a table of **payment attempts**
  where `is_completed = true` identifies successful purchases.
- If new transition patterns appear in future data loads (e.g. `failed →
  completed` retries), the `was_ever_*` flags could be reintroduced. The
  `stg_raw__orders` staging model preserves the full history for this purpose.
