param([string]$FlowplaneRoot = 'C:\FlowPlaneNew\repositories\flowplane-controlplane')
& 'C:\FlowPlaneNew\video-generation-scripts-copy\scripts\demo\11-run-live-local-verification.ps1' -FlowplaneRoot $FlowplaneRoot -Execute -Integration kafka-connect
