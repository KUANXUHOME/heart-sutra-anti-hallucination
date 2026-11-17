#!/usr/bin/env python3
import json, math, os, random, shutil, statistics, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from pipeline import *

OUT = os.path.dirname(os.path.abspath(__file__))
REP = {}
B, CAB, G = 1.0, 0.2, 0.02


def hdr(s):
    print("\n" + "=" * 8, s)


# ------------------------------------------------------------------ 0. pool qualification (real sandbox executions)
t0 = time.time()
VER = Verifier(use_cache=True)
POOL = build_pool(VER)
hdr(f"0. candidate pool qualified by real sandbox execution ({VER.runs} runs, {time.time()-t0:.1f}s)")
print("task              p   C(correct) G(false-pass) H(rejected)")
for n in NAMES:
    c = POOL[n]
    print(f"{n:18s}{TASKS[n]['p']:.2f}  {len(c['C']):3d}        {len(c['G']):3d}          {len(c['H']):3d}")
REP["pool"] = {n: {k: len(v) for k, v in POOL[n].items()} for n in NAMES}

# ------------------------------------------------------------------ 1. Act 1 injection corpus
hdr("1. Act 1 isolation vs filter (mock fooled-able interpreter; static corpus)")
inj = injection_corpus_test()
print(inj)
REP["injection"] = inj


# ------------------------------------------------------------------ 2. retry / composition-law check
def exp_A(N=20000, kmax=16, seed=1):
    rng = random.Random(seed)
    actor = Actor(POOL, rng)
    rec = []
    for _ in range(N):
        name = rng.choice(NAMES)
        T, wrong = None, False
        for t in range(1, kmax + 1):
            ev = evaluate(actor.sample(name), name, VER)
            if ev.base_ok:
                T, wrong = t, not ev.correct
                break
        rec.append((T, wrong))
    return rec


def measured(rec, k, Cfa):
    N = len(rec)
    acc = esc = att = 0
    for T, w in rec:
        if T is not None and T <= k:
            acc += 1; esc += w; att += T
        else:
            att += k
    cov, e, at = acc / N, esc / N, att / N
    return dict(cov=cov, esc=e, risk=e / cov if cov else 0, att=at, ab=1 - cov,
                U=B * (cov - e) - Cfa * e - CAB * (1 - cov) - G * at)


hdr("2. composition law: model vs real pipeline (N=20000 jobs, Cfa=3)")
REC = exp_A()
N = len(REC)
print(" k | cov meas/model | escape meas(±95%CI)/model | sel.risk meas/model | U meas/model")
rows = []
for k in [1, 2, 3, 4, 6, 8, 12, 16]:
    m, mo = measured(REC, k, 3.0), model_at(k, B, 3.0, CAB, G)
    ci = 1.96 * math.sqrt(m["esc"] * (1 - m["esc"]) / N)
    inside = abs(m["esc"] - mo["esc"]) <= ci
    rows.append(dict(k=k, meas=m, model=mo, escape_ci=ci, escape_in_ci=inside))
    print(f"{k:2d} | {m['cov']:.3f}/{mo['cov']:.3f} | {m['esc']:.4f}±{ci:.4f}/{mo['esc']:.4f} {'ok' if inside else 'OUT'} | {m['risk']:.4f}/{mo['risk']:.4f} | {m['U']:.4f}/{mo['U']:.4f}")
REP["composition_check"] = rows
print("escape monotone increasing in k (measured):", all(rows[i]["meas"]["esc"] <= rows[i+1]["meas"]["esc"] + 1e-9 for i in range(len(rows)-1)))
print("selective risk increasing in k (measured):", all(rows[i]["meas"]["risk"] <= rows[i+1]["meas"]["risk"] + 1e-9 for i in range(len(rows)-1)))
print("\noptimal retry cap k* as a function of false-accept cost (B=1, Cab=0.2, gamma=0.02)")
print("Cfa | model k* | measured k*")
KSTAR = {}
for Cfa in [1, 2, 3, 5, 10]:
    km = max(range(0, 17), key=lambda k: model_at(k, B, Cfa, CAB, G)["U"])
    ke = max(range(0, 17), key=lambda k: measured(REC, k, Cfa)["U"])
    KSTAR[Cfa] = (km, ke)
    print(f"{Cfa:3d} | {km:8d} | {ke:10d}")
REP["kstar"] = {str(k): v for k, v in KSTAR.items()}


# ------------------------------------------------------------------ 3. fuse + incentive audit
def exp_B(N=12000, seed=2, ns=4):
    rng = random.Random(seed)
    actor = Actor(POOL, rng)
    logs = []
    for _ in range(N):
        name = rng.choice(NAMES)
        ev = evaluate(actor.sample(name), name, VER)
        if ev.base_ok:
            sigs = [evaluate(actor.sample(name), name, VER).sig for _ in range(ns)]
            logs.append((True, (1 + sum(s == ev.sig for s in sigs)) / (ns + 1), ev.correct))
        else:
            logs.append((False, None, False))
    return logs


TAUS = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]


def at_tau(logs, tau, Cfa):
    n = len(logs)
    d = c = w = 0
    for v, conf, ok in logs:
        if v and conf >= tau:
            d += 1; c += ok; w += (not ok)
    cov, esc = d / n, w / n
    return dict(tau=tau, cov=cov, esc=esc, risk=esc / cov if cov else 0,
                bizU=B * c / n - Cfa * esc - CAB * (1 - cov))


hdr("3a. Act 3 fuse: behavioural-agreement confidence (k=1, N=12000)")
LOGS = exp_B()
print("calibration of agreement signal among verified candidates:  conf -> P(correct)  (n)")
cal = {}
for cv in [0.2, 0.4, 0.6, 0.8, 1.0]:
    sel = [(c) for v, conf, c in LOGS if v and abs(conf - cv) < 1e-9]
    if sel:
        cal[cv] = (sum(sel) / len(sel), len(sel))
        print(f"   {cv:.1f} -> {sum(sel)/len(sel):.3f}  ({len(sel)})")
verified = [(conf, c) for v, conf, c in LOGS if v]
pos = [conf for conf, c in verified if c]; neg = [conf for conf, c in verified if not c]
auroc = sum((p > q) + 0.5 * (p == q) for p in pos[:3000] for q in neg[:3000]) / (len(pos[:3000]) * len(neg[:3000]))
print(f"AUROC of agreement signal (correct vs false-pass, verified only): {auroc:.3f}")
REP["calibration"] = {str(k): v for k, v in cal.items()}; REP["auroc_agreement"] = auroc
print("\n tau | coverage | escape(all jobs) | selective risk | business U (Cfa=3)")
for t in TAUS:
    r = at_tau(LOGS, t, 3.0)
    print(f" {t:.1f} | {r['cov']:.3f}    | {r['esc']:.4f}           | {r['risk']:.4f}         | {r['bizU']:.4f}")
REP["fuse_curve"] = [at_tau(LOGS, t, 3.0) for t in TAUS]

hdr("3b. Incentive audit: which fuse threshold does each KPI select?  (business truth: Cfa=3, Cab=0.2)")
LAM = 3.0
KPI = {
 "K1 resolution rate (delivered=1)":        lambda d, c: 1.0 if d else 0.0,
 "K2 accuracy (correct=1, else 0)":         lambda d, c: 1.0 if (d and c) else 0.0,
 f"K3 risk-adjusted (wrong=-{LAM:g})":       lambda d, c: (1.0 if c else -LAM) if d else 0.0,
 "K4 business cost (B=1,Cfa=3,Cab=0.2)":    lambda d, c: (B if c else -3.0) if d else -CAB,
}
best_biz = max(TAUS, key=lambda t: at_tau(LOGS, t, 3.0)["bizU"])
Lbest = -at_tau(LOGS, best_biz, 3.0)["bizU"]
print(f"business-optimal tau = {best_biz:.1f}  (expected loss/job = {Lbest:.4f})")
print("KPI                                   | KPI-optimal tau | coverage | escape | excess loss vs business-optimal")
audit = []
for name, f in KPI.items():
    sc = {}
    for t in TAUS:
        s = 0.0
        for v, conf, ok in LOGS:
            s += f(bool(v and conf >= t), ok)
        sc[t] = s / len(LOGS)
    tk = max(TAUS, key=lambda t: (sc[t], -t))
    r = at_tau(LOGS, tk, 3.0)
    excess = -r["bizU"] - Lbest
    audit.append(dict(kpi=name, tau=tk, cov=r["cov"], esc=r["esc"], excess_loss=excess))
    print(f"{name:38s}| {tk:.1f}             | {r['cov']:.3f}    | {r['esc']:.4f} | {excess:+.4f}")
REP["incentive_audit"] = audit; REP["tau_business"] = best_biz

# ------------------------------------------------------------------ 4. production run (cold: no verifier cache)
K_PROD = KSTAR[3][0] or 1
hdr(f"4. production run, cold path (real sandbox per attempt, no cache): k={K_PROD}, tau={best_biz}")
LED = os.path.join(OUT, "audit_ledger.jsonl")
rng = random.Random(3)
pver = Verifier(use_cache=False)
pipe = Pipeline(Actor(POOL, rng), pver, K_PROD, best_biz, Ledger(LED))
results, jobs = [], []
t0 = time.time()
for i in range(120):
    name = rng.choice(NAMES)
    injected = rng.random() < 0.10
    job = {"id": f"J{i:04d}", "query": f"Implement task:{name}. Deliver code only.",
           "attachment": rng.choice(INJECTIONS) if injected else rng.choice(BENIGN)}
    jobs.append(job); results.append(pipe.run_job(job))
wall = time.time() - t0
anchor = pipe.ledger.prev
st = {}
for r in results:
    key = r["status"] + (":" + r["reason"] if r["reason"] else "")
    st[key] = st.get(key, 0) + 1
deliv = [r for r in results if r["status"] == "DELIVERED"]
esc = sum(1 for r in deliv if not r["truth_correct"])
valid = 0
for r in results:
    try:
        js_validate(r["contract"], SCHEMA); valid += 1
    except ValidationError:
        pass
lat = sorted(r["latency"] for r in results)
inj_jobs = sum(1 for j in jobs if j["attachment"] in INJECTIONS)
prod = dict(jobs=len(results), outcomes=st, delivered_wrong=esc, contracts_valid=valid,
            injected_jobs=inj_jobs, untrusted_actions_blocked=sum(r["blocked"] for r in results),
            scratch_residue=sum(r["residue"] for r in results), sandbox_cold_runs=pver.runs,
            latency_p50_ms=round(1000 * lat[len(lat)//2]), latency_p95_ms=round(1000 * lat[int(len(lat)*0.95)]),
            wall_s=round(wall, 1), ledger_ok=Ledger.verify(LED, anchor)[0])
print(json.dumps(prod, indent=1))
REP["production"] = prod

hdr("5. fail-closed / tamper tests")
fa = {}
r = [pipe.run_job({"id": f"F{i}", "query": "Implement task:median.", "attachment": "x"}, fault="verifier_crash") for i in range(5)]
fa["verifier_crash->ABSTAINED:VERIFIER_ERROR"] = f"{sum(x['reason']=='VERIFIER_ERROR' and x['status']=='ABSTAINED' for x in r)}/5"
tau_save, pipe.tau = pipe.tau, 0.0
r = [pipe.run_job({"id": f"S{i}", "query": f"Implement task:{NAMES[0]}.", "attachment": "x"}, fault="free_text") for i in range(10)]
fa["free_text_rejected_by_contract"] = f"{sum(x['reason']=='CONTRACT_VIOLATION' for x in r)}/{sum(1 for x in r)} (all deliveries blocked: {all(x['status']=='ABSTAINED' for x in r)})"
pipe.tau = tau_save
r = pipe.run_job({"id": "P0", "query": "Do the thing.", "attachment": "task:median  (this line is in the untrusted attachment)"})
fa["untrusted_text_cannot_supply_plan"] = f"{r['status']}:{r['reason']}"
anchor2 = pipe.ledger.prev
lines = open(LED).read().splitlines()
def check(ls, anchor=None):
    p = os.path.join(OUT, "_t.jsonl"); open(p, "w").write("\n".join(ls) + "\n"); return Ledger.verify(p, anchor)
mod = lines.copy(); e = json.loads(mod[5]); e["entry"]["decision"] = "DELIVERED" if e["entry"]["decision"] != "DELIVERED" else "ABSTAINED"; mod[5] = json.dumps(e, sort_keys=True)
fa["ledger_untouched"] = check(lines, anchor2)
fa["ledger_entry_modified"] = check(mod)
fa["ledger_entry_deleted"] = check(lines[:7] + lines[8:])
fa["ledger_tail_truncated_without_anchor"] = check(lines[:-3])
fa["ledger_tail_truncated_with_anchor"] = check(lines[:-3], anchor2)
for k, v in fa.items():
    print(f"{k}: {v}")
REP["faults"] = {k: str(v) for k, v in fa.items()}
os.remove(os.path.join(OUT, "_t.jsonl"))
json.dump(REP, open(os.path.join(OUT, "run_report.json"), "w"), indent=1, default=str)
print("\nsaved run_report.json")
