# gRPC status

The in-process runtime contract fixture produced byte-identical valid output and canonical invalid-input behavior for gRPC batch and stream modes. That contract test passes.

The latest preserved live gRPC service attempts did not pass: the service returned `UNIMPLEMENTED` for unary and streaming operations. Public claims must therefore say “contract parity demonstrated; live service proof failed,” not “gRPC verified.”
