# Payload scaling: 1-64 MiB

The controlled JMH run used the same frozen 976-field mapping at every size. Mean time grew from 0.473 ms at 1 MiB to 19.387 ms at 64 MiB. A linear regression gives a 0.301666 ms/MiB slope, 0.276089 ms intercept, and R² 0.998895.

Allocation averaged 189,375.68 B/op and varied by 1.785% across sizes because only an unreferenced bulk field grew; normalized output remained 36,478 bytes. This is a scanner-scaling result, not proof of constant allocation for arbitrary mappings.

A realistic 16 MiB referenced-field checkpoint materialized a 17.2 MB output, took 178.765 ms, and allocated 182,349,417 B/op.

See [results](results.csv), [regression](regression.json), preserved raw [averages](raw-average-results.json), [allocation](raw-allocation-results.json), and [percentiles](raw-percentiles.json).
