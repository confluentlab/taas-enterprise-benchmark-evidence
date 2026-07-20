# Core benchmark methodology

The benchmark consumes raw fixture bytes. Each operation performs the full scan/parse, resolves and executes 976 compiled mapping fields, applies transformations and policy/error behavior, and serializes an owned output byte array. Mapping compilation and corpus construction occur before timing.

“Full scan” means the parser/scanner consumes the complete input byte sequence. It does not mean every input field is materialized into the output model. The payload-scaling fixture demonstrates this distinction: its added bulk field is scanned but not referenced by a compiled mapping field.

The controlled publication protocol used 12 launches with 30-second cooldowns. Success and bounded-error modes were evaluated independently. Groups A, B, and C each aggregate three fresh launches for clean means, percentiles, and allocation evidence. Group D enables sample-level diagnostic profiling whose instrumentation may alter timing, so it is retained for analysis but excluded from publication qualification. Repeatability checks compare within-group spread and independent group aggregates. All 12 must pass for publication qualification.

Nine checks met their limits. Three mean-latency spread checks missed the 5% limit: success group B observed 7.763%, bounded-error group A observed 5.007%, and bounded-error group B observed 8.525%. The public status is `MEASURED_NOT_QUALIFIED`; the original evaluator status `PROVISIONAL_REJECTED` remains preserved in the machine-readable qualification file.
