This repository contains the reference implementation of the Heart Sutra Four-Act Anti-Hallucination Engineering Chain.
Read the paper first: https://zenodo.org/records/24286070

Current Status: The current Python pipeline is a verification prototype (using a controlled mutation-based stand-in Actor). It successfully proves the composition law and the KPI incentive gap (as detailed in Section 8 of the paper).

What we need (for industrial grade):
1. Real LLM Integration: Replace the stand-in Actor with real LLM adapters (OpenAI, Anthropic, local LLMs via vLLM).
2. Sandbox Hardening: Upgrade the current subprocess sandbox to gVisor / Firecracker (Section 10, limitation 7).
3. Ledger External Anchoring: Implement WORM (Write Once Read Many) storage or transparency logs for the hash-chain (Section 10, limitation 8).

If you are an engineer interested in AI safety, sandboxing, or distributed ledgers, please check the Issues tab and submit a PR. Let's run the minimal real experiments (Section 10) together.
