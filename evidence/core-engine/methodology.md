# Core benchmark methodology

The benchmark consumes raw fixture bytes. Each operation performs the full scan/parse, resolves and executes a precompiled 976-field mapping, applies transformations and policy/error behavior, and serializes an owned output byte array. Mapping compilation and corpus construction occur before timing.

The controlled publication protocol used 12 launches with 30-second cooldowns. Success and bounded-error modes were evaluated independently. Groups A, B, and C each aggregate three fresh launches for clean means, percentiles, and allocation evidence; group D is diagnostic only. Repeatability checks compare within-group spread and independent group aggregates. All 12 must pass for publication qualification.

Nine checks met their limits. Three mean-latency spread checks missed the 5% limit: success group B observed 7.763%, bounded-error group A observed 5.007%, and bounded-error group B observed 8.525%. The public status is `MEASURED_NOT_QUALIFIED`; the original evaluator status `PROVISIONAL_REJECTED` remains preserved in the machine-readable qualification file.
