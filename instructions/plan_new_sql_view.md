# How to Add a New SQL View to qDash

## 1. Create the Main View

- Add a new file: `sql/<view_name>.sql`
- Follow naming convention: `vw_<descriptive_name>`

## 2. Create the Materialized View

- Edit `sql/materialized_views.sql`
- In section **"1. Intermediate materialized views"**, add:

```sql
CREATE MATERIALIZED VIEW public.mv_my_new_view AS
SELECT * FROM public.vw_my_new_view;

CREATE UNIQUE INDEX mv_my_new_view_<key>_uniq
    ON public.mv_my_new_view (<unique_key_columns>);
```

- Choose the unique index columns based on the view's grain
  (e.g. `(date)` for one-row-per-date, `(date, symbol)` for per-ETF).

## 3. Update `refresh_etf_matviews()` Function

- In `sql/materialized_views.sql`, find the `refresh_etf_matviews()` function body.
- Add a refresh block in the correct position:
  - Intermediate matviews refresh **BEFORE** report matviews.
  - Report matviews refresh **AFTER** intermediates.
- Verify:

```sql
SELECT refresh_etf_matviews();
SELECT count(*) FROM mv_my_new_view;
```
