import random, math, json, sys
from pipeline import *
VER=Verifier(); POOL=build_pool(VER); NS=4
def conf_of(actor,name,ev):
    sigs=[evaluate(actor.sample(name),name,VER).sig for _ in range(NS)]
    return (1+sum(s==ev.sig for s in sigs))/(NS+1)
# 1) estimate per-task operating characteristics alpha_i(tau)=P(conf>=tau|correct), beta_i(tau)=P(conf>=tau|false-pass) on an independent sample
rng=random.Random(101); actor=Actor(POOL,rng); M=6000
TAUS=[0.4,0.6,0.8]
AB={}
for name in NAMES:
    cs=[];ws=[]
    for _ in range(M):
        ev=evaluate(actor.sample(name),name,VER)
        if ev.base_ok:
            (cs if ev.correct else ws).append(conf_of(actor,name,ev))
    AB[name]={t:(sum(c>=t for c in cs)/len(cs), (sum(c>=t for c in ws)/len(ws)) if ws else 0.0) for t in TAUS}
# 2) model prediction
def model(k,tau):
    esc=cov=0
    for name in NAMES:
        p=TASKS[name]["p"]; s=s_of(p); c,w,r=1-p,p*s,p*(1-s); q=w/(c+w); a,b=AB[name][tau]
        A=1-r**k; esc+=A*q*b; cov+=A*((1-q)*a+q*b)
    return cov/len(NAMES), esc/len(NAMES)
# 3) measured with the real pipeline logic on fresh jobs
def measured(k,tau,N=30000,seed=202):
    rng=random.Random(seed); actor=Actor(POOL,rng); d=e=0
    for _ in range(N):
        name=rng.choice(NAMES)
        for t in range(k):
            ev=evaluate(actor.sample(name),name,VER)
            if ev.base_ok:
                if conf_of(actor,name,ev)>=tau: d+=1; e+=(not ev.correct)
                break
    return d/N,e/N,N
print(" k tau | coverage meas/model | escape meas(±95%CI)/model")
out=[]
for k in [1,2,3]:
    for tau in TAUS:
        mc,me,N=measured(k,tau); pc,pe=model(k,tau); ci=1.96*math.sqrt(me*(1-me)/N)
        ok=abs(me-pe)<=ci
        out.append(dict(k=k,tau=tau,cov_meas=mc,cov_model=pc,esc_meas=me,esc_model=pe,ci=ci,inside=ok))
        print(f"{k:2d} {tau:.1f} | {mc:.3f}/{pc:.3f} | {me:.4f}±{ci:.4f}/{pe:.4f} {'ok' if ok else 'OUT'}")
json.dump(out,open("fuse_check.json","w"),indent=1)
