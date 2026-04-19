# ETF Metadata Decision Tree Prompt Plan

You are filling missing values in etf_metadata for one ETF at a time.

Your job:
- Use the existing etf_metadata table as the taxonomy reference.
- Do not invent new labels unless explicitly allowed.
- For each missing column, decide one of:
  - fill with an existing taxonomy value
  - leave blank because the field is not applicable
  - uncertain, needs manual review

Required process for each ETF:
1. Identify the ETF's exposure type:
   - equity sector
   - broad equity index
   - country equity
   - regional equity
   - bonds
   - commodity
   - crypto
   - currency
   - thematic equity
2. Infer what each missing field means for that exposure type.
3. Compare against similar ETFs already populated in etf_metadata.
4. For each missing field, output:
   - decision
   - proposed value if filled
   - confidence: high / medium / low
   - short rationale
5. Prefer blank over weak inference.

Rules:
- `duration_bucket` and `credit_bucket` are generally only for bond ETFs.
- `commodity_group` is only for direct commodity or strong commodity-linked exposures.
- `country` should be blank for global or multi-country exposures unless one country clearly dominates the fund definition.
- `benchmark_group` should only be filled if the benchmark family is clear and matches existing taxonomy.
- `style` should reflect portfolio behavior or exposure style, not marketing language.
- If the field is conceptually not a fit, leave it blank.
- Do not optimize for completeness. Optimize for correctness.

Output as a table with:
symbol | column | decision | value | confidence | rationale
```

## Example Framing for URA

```text
ETF: URA
Known context:
- Uranium-focused equity ETF
- Missing columns: country, style, commodity_group, duration_bucket, credit_bucket, benchmark_group

Think column by column:
- country: is this single-country or global?
- style: is there a clear existing style label in the table that fits uranium miners/equity commodity exposure?
- commodity_group: does uranium belong in an existing commodity_group already present in the table, or should this remain blank?
- duration_bucket: applicable or not?
- credit_bucket: applicable or not?
- benchmark_group: is there a known benchmark family that matches current taxonomy, or should blank remain?

Also compare against similar rows such as:
- GDX
- LITP
- XLE
- GLD
- DBA
```

- the row being fixed
- the distinct allowed values for each column
- 3-8 similar populated ETFs
- a rule that blank is acceptable and often correct

