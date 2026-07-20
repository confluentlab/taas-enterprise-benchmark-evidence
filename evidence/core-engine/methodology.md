# Core benchmark methodology

The benchmark consumes raw fixture bytes. Each operation performs the full scan/parse, resolves and executes a precompiled 976-field mapping, applies transformations and policy/error behavior, and serializes an owned output byte array. Mapping compilation and corpus construction occur before timing.

The controlled publication protocol used 12 launches with 30-second cooldowns. Success and bounded-error modes were evaluated independently. Repeatability gates compare launch groups and spreads; all must pass for publication eligibility. Three mean-spread gates failed, so the correct status is `PROVISIONAL_REJECTED` even though functional tests and raw hashes passed.
